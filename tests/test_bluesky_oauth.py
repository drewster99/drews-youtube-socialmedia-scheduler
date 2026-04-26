"""Tests for ``services/bluesky_oauth.py``.

The actual OAuth handshake against ``bsky.social`` isn't exercised here
— that requires a publicly-reachable server and a live AS. We do verify
every piece of crypto + parsing the rest of the flow depends on, so a
regression in (e.g.) DPoP proof signing surfaces in CI rather than at
post time.
"""

from __future__ import annotations

import base64
import hashlib
import json

import pytest
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import (
    decode_dss_signature,
)
from cryptography.hazmat.primitives.hashes import SHA256

from yt_scheduler.services import bluesky_oauth as bo


# --- Handle normalisation --------------------------------------------------


def test_normalise_handle_strips_at_and_lowercases() -> None:
    assert bo.normalise_handle("@Alice.BSKY.Social") == "alice.bsky.social"


def test_normalise_handle_rejects_no_dot() -> None:
    assert bo.normalise_handle("alice") == ""


def test_normalise_handle_rejects_invalid_chars() -> None:
    assert bo.normalise_handle("alice bob.bsky.social") == ""
    assert bo.normalise_handle("alice/bob.bsky.social") == ""


def test_normalise_handle_accepts_custom_domain() -> None:
    assert bo.normalise_handle("blog.example.com") == "blog.example.com"


# --- ES256 keypair + JWK ---------------------------------------------------


def test_generate_keypair_pem_round_trips() -> None:
    pem = bo.generate_keypair_pem()
    assert "BEGIN PRIVATE KEY" in pem
    key = bo.load_private_key(pem)
    assert isinstance(key, ec.EllipticCurvePrivateKey)
    assert key.curve.name == "secp256r1"


def test_public_jwk_has_correct_shape() -> None:
    pem = bo.generate_keypair_pem()
    key = bo.load_private_key(pem)
    jwk = bo.public_jwk(key)
    assert jwk["kty"] == "EC"
    assert jwk["crv"] == "P-256"
    # x and y are 32 bytes each → 43 base64url chars (no padding)
    assert len(bo._b64url_decode(jwk["x"])) == 32
    assert len(bo._b64url_decode(jwk["y"])) == 32


def test_jwk_thumbprint_is_deterministic() -> None:
    pem = bo.generate_keypair_pem()
    key = bo.load_private_key(pem)
    jwk = bo.public_jwk(key)
    a = bo.jwk_thumbprint(jwk)
    b = bo.jwk_thumbprint(jwk)
    assert a == b
    # Length matches a base64url-encoded SHA-256 (32 bytes → 43 chars)
    assert len(bo._b64url_decode(a)) == 32


# --- DPoP proof JWT --------------------------------------------------------


def _decode_jwt(token: str) -> tuple[dict, dict, bytes, bytes]:
    """Return (header, payload, signing_input, raw_signature)."""
    header_b64, payload_b64, sig_b64 = token.split(".")
    header = json.loads(bo._b64url_decode(header_b64))
    payload = json.loads(bo._b64url_decode(payload_b64))
    signing_input = f"{header_b64}.{payload_b64}".encode("ascii")
    sig = bo._b64url_decode(sig_b64)
    return header, payload, signing_input, sig


def test_dpop_proof_header_shape() -> None:
    pem = bo.generate_keypair_pem()
    proof = bo.sign_dpop_proof(pem, "POST", "https://example.test/par")
    header, payload, _, _ = _decode_jwt(proof)
    assert header["typ"] == "dpop+jwt"
    assert header["alg"] == "ES256"
    jwk = header["jwk"]
    assert jwk["kty"] == "EC" and jwk["crv"] == "P-256"
    assert payload["htm"] == "POST"
    assert payload["htu"] == "https://example.test/par"
    assert "iat" in payload
    assert "jti" in payload
    assert "nonce" not in payload  # not requested


def test_dpop_proof_includes_nonce_and_ath_when_given() -> None:
    pem = bo.generate_keypair_pem()
    proof = bo.sign_dpop_proof(
        pem, "POST", "https://example.test/x?a=1#b",
        nonce="srv-nonce-abc",
        access_token="bearer-xyz",
    )
    _, payload, _, _ = _decode_jwt(proof)
    assert payload["nonce"] == "srv-nonce-abc"
    expected_ath = base64.urlsafe_b64encode(
        hashlib.sha256(b"bearer-xyz").digest()
    ).rstrip(b"=").decode("ascii")
    assert payload["ath"] == expected_ath
    # htu has query+fragment stripped
    assert payload["htu"] == "https://example.test/x"


def test_dpop_proof_signature_verifies_against_embedded_jwk() -> None:
    pem = bo.generate_keypair_pem()
    proof = bo.sign_dpop_proof(pem, "POST", "https://example.test/par")
    header, _, signing_input, raw_sig = _decode_jwt(proof)
    # Reconstruct the public key from the embedded JWK
    jwk = header["jwk"]
    x = int.from_bytes(bo._b64url_decode(jwk["x"]), "big")
    y = int.from_bytes(bo._b64url_decode(jwk["y"]), "big")
    public_numbers = ec.EllipticCurvePublicNumbers(x, y, ec.SECP256R1())
    public_key = public_numbers.public_key()
    # Raw R||S → DER for cryptography's verify()
    r = int.from_bytes(raw_sig[:32], "big")
    s = int.from_bytes(raw_sig[32:], "big")
    from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature
    public_key.verify(encode_dss_signature(r, s), signing_input, ec.ECDSA(SHA256()))


def test_dpop_proof_signatures_are_deterministically_decodeable() -> None:
    """The raw R||S signature must be decodeable by cryptography's
    decode_dss_signature without errors (round-trips)."""
    pem = bo.generate_keypair_pem()
    proof = bo.sign_dpop_proof(pem, "GET", "https://example.test/x")
    _, _, _, raw_sig = _decode_jwt(proof)
    assert len(raw_sig) == 64
    r = int.from_bytes(raw_sig[:32], "big")
    s = int.from_bytes(raw_sig[32:], "big")
    # Smoke-check that decode_dss_signature works on a re-encoded form.
    from cryptography.hazmat.primitives.asymmetric.utils import (
        encode_dss_signature,
    )
    decode_dss_signature(encode_dss_signature(r, s))


# --- PKCE ------------------------------------------------------------------


def test_pkce_pair_matches_S256() -> None:
    verifier, challenge = bo.make_pkce_pair()
    expected = base64.urlsafe_b64encode(
        hashlib.sha256(verifier.encode("ascii")).digest()
    ).rstrip(b"=").decode("ascii")
    assert challenge == expected
    # Verifier should be ≥43 chars per RFC 7636
    assert len(verifier) >= 43


# --- Localhost client_id ----------------------------------------------------


def test_localhost_client_id_encodes_redirect_uri_and_scope() -> None:
    cid = bo.localhost_client_id(
        "http://127.0.0.1:8008/api/oauth/bluesky/callback"
    )
    assert cid.startswith("http://localhost?")
    assert "redirect_uri=http%3A%2F%2F127.0.0.1%3A8008%2F" in cid
    assert "scope=atproto%20transition%3Ageneric" in cid


# --- DID document → PDS endpoint -------------------------------------------


def test_pds_endpoint_from_did_document_finds_atproto_pds() -> None:
    doc = {
        "service": [
            {"id": "#other", "type": "Other", "serviceEndpoint": "https://nope.test"},
            {
                "id": "#atproto_pds",
                "type": "AtprotoPersonalDataServer",
                "serviceEndpoint": "https://pds.example.test/",
            },
        ]
    }
    assert bo.pds_endpoint_from_did_document(doc) == "https://pds.example.test"


def test_pds_endpoint_from_did_document_raises_when_missing() -> None:
    with pytest.raises(ValueError, match="atproto_pds"):
        bo.pds_endpoint_from_did_document({"service": []})


def test_pds_endpoint_from_did_document_handles_unprefixed_id() -> None:
    """Some DID documents use ``id: "atproto_pds"`` without the leading #."""
    doc = {
        "service": [
            {
                "id": "atproto_pds",
                "type": "AtprotoPersonalDataServer",
                "serviceEndpoint": "https://pds2.example.test",
            }
        ]
    }
    assert bo.pds_endpoint_from_did_document(doc) == "https://pds2.example.test"


# --- Bundle shape ----------------------------------------------------------


def test_credentialed_bundle_has_required_keys() -> None:
    bundle = bo.credentialed_bundle(
        handle="alice.test",
        did="did:plc:abc",
        pds="https://pds.test",
        auth_server_issuer="https://bsky.social",
        token_endpoint="https://bsky.social/oauth/token",
        redirect_uri="http://127.0.0.1:8008/api/oauth/bluesky/callback",
        private_key_pem="pem",
        access_token="atok",
        refresh_token="rtok",
        expires_in=7200,
    )
    for key in (
        "auth_method", "handle", "did", "pds", "private_key_pem",
        "access_token", "refresh_token", "token_endpoint", "redirect_uri",
        "expires_at",
    ):
        assert key in bundle
    assert bundle["auth_method"] == "oauth"
    assert bundle["expires_at"] > 0
