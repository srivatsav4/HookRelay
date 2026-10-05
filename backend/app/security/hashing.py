"""API-key and password hashing. Pure stdlib."""
from __future__ import annotations

import hashlib
import hmac
import secrets


def generate_api_key() -> tuple[str, str]:
    """Return (raw_key, prefix). Raw key is shown once and never stored."""
    prefix = secrets.token_hex(4)
    return f"hr_{prefix}_{secrets.token_urlsafe(32)}", prefix


def parse_api_key(raw: str) -> str | None:
    parts = raw.split("_", 2)
    if len(parts) != 3 or parts[0] != "hr" or len(parts[1]) != 8 or not parts[2]:
        return None
    return parts[1]


def hash_api_key(raw: str, pepper: str) -> str:
    # Keys are 256-bit random values, so a fast keyed hash is appropriate (no need for bcrypt/scrypt stretching).
    return hmac.new(pepper.encode(), raw.encode(), hashlib.sha256).hexdigest()


def verify_api_key(raw: str, stored_hash: str, pepper: str) -> bool:
    return hmac.compare_digest(hash_api_key(raw, pepper), stored_hash)


_N, _R, _P = 2**14, 8, 1


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    dk = hashlib.scrypt(password.encode(), salt=salt, n=_N, r=_R, p=_P)
    return f"scrypt${_N}${_R}${_P}${salt.hex()}${dk.hex()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        _, n, r, p, salt, dk = stored.split("$")
        calc = hashlib.scrypt(password.encode(), salt=bytes.fromhex(salt), n=int(n), r=int(r), p=int(p))
    except (ValueError, TypeError):
        return False
    return hmac.compare_digest(calc.hex(), dk)
