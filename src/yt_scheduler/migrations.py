"""SQLite migration runner.

Migrations are plain `.sql` files in the repo's top-level `migrations/` directory,
named `NNN_<slug>.sql` where NNN is a zero-padded integer version number starting
at 001. The runner applies each pending file in a single atomic transaction per
file: all DDL/DML statements plus the `schema_migrations` stamp are committed
together, so a mid-migration failure leaves the schema unchanged and the version
unstamped, letting the next startup safely retry.

``PRAGMA`` statements (e.g. ``foreign_keys = OFF/ON`` used by table-rebuild
migrations) are non-transactional in SQLite — they are silently ignored when
issued inside an active transaction. The runner therefore executes any
``PRAGMA`` lines outside the ``BEGIN``/``COMMIT`` envelope. ``PRAGMA
foreign_keys`` is restored to ON unconditionally in the ``finally`` block
regardless of success or failure.

On a pre-existing database, ``001_baseline.sql`` simply re-runs: it is entirely
``CREATE TABLE IF NOT EXISTS`` (no indexes/triggers), so it is a no-op for tables
that already exist and is then recorded as version 1. We do not stamp it by
table-name match — that would falsely mark a partially-built schema as fully
migrated.
"""

from __future__ import annotations

import hashlib
import logging
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path

import aiosqlite

logger = logging.getLogger(__name__)

_FILENAME_RE = re.compile(r"^(\d{3,})_([a-z0-9_]+)\.sql$")

def _resolve_migrations_dir() -> Path:
    """Locate the migrations directory in dev *and* inside the bundled .app.

    Search order:

    * ``<package>/_migrations`` — bundled location (build.sh copies the SQL
      files here so the .app is self-contained).
    * ``<package>/../migrations`` — when the package is laid out next to a
      sibling ``migrations`` dir (e.g. when ``yt_scheduler_src/`` contains both
      ``yt_scheduler/`` and ``migrations/``).
    * ``<repo-root>/migrations`` — dev mode, running from a checkout.
    """
    here = Path(__file__).resolve().parent

    bundled_inside = here / "_migrations"
    if bundled_inside.exists():
        return bundled_inside

    bundled_sibling = here.parent / "migrations"
    if bundled_sibling.exists():
        return bundled_sibling

    return here.parents[1] / "migrations"  # repo root in dev


MIGRATIONS_DIR = _resolve_migrations_dir()


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    path: Path
    sql: str

    @property
    def checksum(self) -> str:
        return hashlib.sha256(self.sql.encode("utf-8")).hexdigest()


def _split_statements(sql: str) -> tuple[list[str], list[str]]:
    """Split a migration SQL file into leading PRAGMA statements and transactional statements.

    Returns ``(leading_pragmas, txn_stmts)``.

    ``leading_pragmas`` are PRAGMA statements that appear before the first
    non-PRAGMA statement in the file.  They must run outside the transaction
    because SQLite silently ignores ``PRAGMA foreign_keys`` (and a few others)
    when issued inside an active transaction.  ``002_projects.sql`` and
    ``008_per_project_credentials.sql`` use ``PRAGMA foreign_keys = OFF`` here
    to allow their DROP/RENAME table-rebuild dance.

    Trailing PRAGMA lines (after the last DDL/DML statement) are intentionally
    excluded: the ``apply_migrations`` finally-block restores FK enforcement
    unconditionally, so including the trailing ``PRAGMA foreign_keys = ON``
    would prematurely turn FKs back ON before the transaction commits and
    re-enable the very CASCADE deletions that ``PRAGMA foreign_keys = OFF``
    was meant to suppress.

    Statements are split with ``sqlite3.complete_statement`` rather than a naive
    ``;`` split, so a semicolon (or ``--``) inside a string literal — e.g. a
    seeded prompt-template body like ``'Step 1; do X'`` — no longer mis-splits a
    statement. (Assumes at most one statement per physical line, which holds for
    every migration here; two statements jammed onto one line would surface as a
    loud "execute one statement at a time" error rather than silent corruption.)
    """
    def _strip_leading_comments(stmt: str) -> str:
        # complete_statement keeps comments, but a standalone `-- ...` line
        # accumulates onto the FOLLOWING statement. Drop leading comment/blank
        # lines so PRAGMA classification (below) sees the real first keyword;
        # inline comments after the first token are left for SQLite to parse.
        lines = stmt.splitlines()
        i = 0
        while i < len(lines) and (not lines[i].strip() or lines[i].strip().startswith("--")):
            i += 1
        return "\n".join(lines[i:]).strip()

    all_stmts: list[str] = []
    buffer = ""
    for line in sql.splitlines(keepends=True):
        buffer += line
        if sqlite3.complete_statement(buffer):
            stmt = _strip_leading_comments(buffer)
            if stmt:
                all_stmts.append(stmt)
            buffer = ""
    tail = _strip_leading_comments(buffer)
    if tail:
        # A non-empty, non-comment leftover never completed: a final statement
        # missing its trailing ';' (every migration here ends in ';', so this is
        # rare). Keep it so a genuinely malformed migration fails loudly instead
        # of silently dropping DDL.
        all_stmts.append(tail)

    # Collect leading PRAGMAs (before the first non-PRAGMA statement).
    leading_pragmas: list[str] = []
    first_non_pragma = 0
    for i, stmt in enumerate(all_stmts):
        if stmt.upper().startswith("PRAGMA"):
            leading_pragmas.append(stmt)
        else:
            first_non_pragma = i
            break

    # All statements from the first non-PRAGMA onward go into the transaction,
    # except trailing PRAGMAs after the last DDL/DML statement. Those are the
    # closing "PRAGMA foreign_keys = ON" fence lines covered by the finally-block.
    remaining = all_stmts[first_non_pragma:]
    last_non_pragma = len(remaining) - 1
    while last_non_pragma >= 0 and remaining[last_non_pragma].upper().startswith("PRAGMA"):
        last_non_pragma -= 1
    txn_stmts = remaining[: last_non_pragma + 1]

    return leading_pragmas, txn_stmts


def discover_migrations(directory: Path = MIGRATIONS_DIR) -> list[Migration]:
    """Return all migrations in `directory` sorted by version.

    Raises if filenames don't match the expected pattern, versions are not
    contiguous starting at 1, or the same version appears twice.
    """
    if not directory.exists():
        return []

    migrations: list[Migration] = []
    seen_versions: set[int] = set()
    for entry in sorted(directory.iterdir()):
        if not entry.is_file() or entry.suffix != ".sql":
            continue
        match = _FILENAME_RE.match(entry.name)
        if not match:
            raise ValueError(
                f"Migration filename {entry.name!r} does not match NNN_<slug>.sql"
            )
        version = int(match.group(1))
        if version in seen_versions:
            raise ValueError(f"Duplicate migration version {version}")
        seen_versions.add(version)
        migrations.append(
            Migration(
                version=version,
                name=match.group(2),
                path=entry,
                sql=entry.read_text(),
            )
        )

    migrations.sort(key=lambda m: m.version)
    for index, migration in enumerate(migrations, start=1):
        if migration.version != index:
            raise ValueError(
                f"Migration versions must be contiguous starting at 1; "
                f"found {migration.version} at position {index}"
            )
    return migrations


async def _ensure_meta_table(conn: aiosqlite.Connection) -> None:
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            checksum TEXT NOT NULL,
            applied_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    await conn.commit()


async def _applied_versions(conn: aiosqlite.Connection) -> dict[int, str]:
    """Return {version: checksum} for migrations recorded as applied."""
    cursor = await conn.execute("SELECT version, checksum FROM schema_migrations")
    rows = await cursor.fetchall()
    return {int(row[0]): row[1] for row in rows}


async def apply_migrations(
    conn: aiosqlite.Connection,
    directory: Path = MIGRATIONS_DIR,
    target_version: int | None = None,
) -> list[int]:
    """Apply pending migrations against `conn`.

    Returns the list of versions that were applied during this call. On a
    pre-existing database the idempotent baseline (001) re-runs as a no-op and is
    recorded like any other migration.

    ``target_version`` is a test hook: when set, migrations newer than that
    version are skipped, letting tests assert behaviour from a specific
    historical migration without later ones reshaping the world.
    """
    migrations = discover_migrations(directory)
    if target_version is not None:
        migrations = [m for m in migrations if m.version <= target_version]
    if not migrations:
        # An empty migrations directory almost always means the SQL files
        # weren't bundled correctly — fail loudly rather than letting a half-
        # initialised DB through to ``ensure_default_project()`` and friends.
        raise RuntimeError(
            f"No migration .sql files found under {directory!s}. "
            "If running from a built .app bundle, this likely means "
            "build.sh didn't copy the migrations directory."
        )

    await _ensure_meta_table(conn)
    applied = await _applied_versions(conn)

    # 001_baseline.sql is entirely `CREATE TABLE IF NOT EXISTS` (no indexes or
    # triggers), so on a pre-existing DB it re-runs as a harmless no-op and is
    # then recorded as version 1. We deliberately do NOT stamp version 1 by
    # table-name match: a partially-built DB that merely had the baseline table
    # *names* (but missing columns) would be falsely marked fully migrated, and
    # every later migration would then run against an incomplete schema.

    newly_applied: list[int] = []
    for migration in migrations:
        if migration.version in applied:
            recorded_checksum = applied[migration.version]
            if recorded_checksum != migration.checksum:
                raise RuntimeError(
                    f"Migration {migration.version:03d}_{migration.name} has been "
                    f"modified after it was applied (checksum mismatch). "
                    f"Roll back via a new migration instead of editing this file."
                )
            continue

        logger.info("Applying migration %03d_%s", migration.version, migration.name)
        leading_pragmas, txn_stmts = _split_statements(migration.sql)
        try:
            # Leading PRAGMAs (e.g. PRAGMA foreign_keys = OFF) must run outside
            # the transaction — SQLite silently ignores foreign_keys changes
            # issued inside an active transaction.
            for pragma in leading_pragmas:
                await conn.execute(pragma)

            # All DDL/DML plus the version stamp go in one atomic transaction.
            # Using explicit BEGIN + per-statement execute() (not executescript)
            # keeps us in control: executescript issues an implicit COMMIT before
            # running, making rollback a no-op on failure.
            await conn.execute("BEGIN")
            try:
                for stmt in txn_stmts:
                    await conn.execute(stmt)
                await conn.execute(
                    "INSERT INTO schema_migrations (version, name, checksum) "
                    "VALUES (?, ?, ?)",
                    (migration.version, migration.name, migration.checksum),
                )
                await conn.execute("COMMIT")
            except Exception:
                await conn.execute("ROLLBACK")
                raise
        finally:
            # PRAGMA foreign_keys is per-connection and non-transactional, so
            # neither COMMIT nor ROLLBACK restores it. Ensure it stays ON
            # regardless of outcome so later migrations and the app aren't
            # silently running with FK enforcement disabled.
            await conn.execute("PRAGMA foreign_keys = ON")
        newly_applied.append(migration.version)

    return newly_applied
