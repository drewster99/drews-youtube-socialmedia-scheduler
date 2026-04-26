"""SQLite migration runner.

Migrations are plain `.sql` files in the repo's top-level `migrations/` directory,
named `NNN_<slug>.sql` where NNN is a zero-padded integer version number starting
at 001. The runner applies each pending file in numeric order inside a single
transaction per file and records the version (with checksum and timestamp) in
`schema_migrations`.

A pre-existing database whose tables already match `001_baseline.sql` is stamped
at version 1 without re-running the statements (so we don't trip over the
implicit `IF NOT EXISTS` baseline that earlier code created).
"""

from __future__ import annotations

import hashlib
import logging
import re
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

# Tables that, if all present, indicate a database matching the baseline schema.
_BASELINE_TABLES = {
    "videos", "social_posts", "templates", "blocklist", "moderation_log", "settings",
}


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    path: Path
    sql: str

    @property
    def checksum(self) -> str:
        return hashlib.sha256(self.sql.encode("utf-8")).hexdigest()


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


async def _table_names(conn: aiosqlite.Connection) -> set[str]:
    cursor = await conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
    )
    rows = await cursor.fetchall()
    return {row[0] for row in rows}


async def apply_migrations(
    conn: aiosqlite.Connection,
    directory: Path = MIGRATIONS_DIR,
    target_version: int | None = None,
) -> list[int]:
    """Apply pending migrations against `conn`.

    Returns the list of versions that were applied during this call. Existing
    databases whose schema already matches the baseline are stamped at version 1
    without re-running the SQL.

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

    # Stamp baseline if the DB already matches but isn't recorded.
    if 1 not in applied:
        existing = await _table_names(conn)
        if _BASELINE_TABLES.issubset(existing):
            baseline = next((m for m in migrations if m.version == 1), None)
            if baseline is not None:
                await conn.execute(
                    "INSERT INTO schema_migrations (version, name, checksum) "
                    "VALUES (?, ?, ?)",
                    (baseline.version, baseline.name, baseline.checksum),
                )
                await conn.commit()
                applied = await _applied_versions(conn)
                logger.info("Stamped existing database at baseline migration 001")

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
        try:
            await conn.executescript(migration.sql)
            await conn.execute(
                "INSERT INTO schema_migrations (version, name, checksum) "
                "VALUES (?, ?, ?)",
                (migration.version, migration.name, migration.checksum),
            )
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise
        newly_applied.append(migration.version)

    return newly_applied
