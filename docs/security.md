# Security

- **API keys**: `hr_<8-hex prefix>_<256-bit secret>`; shown once; stored as HMAC-SHA256(pepper, key). Prefix is a lookup handle. Constant-time compare.
- **Management auth**: JWT (HS256, 1h). Passwords: scrypt (stdlib). Login does equal work for unknown users.
- **Signing**: `X-HookRelay-Signature: v1=HMAC_SHA256(secret, "{timestamp}." + raw_body)`, `X-HookRelay-Timestamp`, 300s tolerance, constant-time compare.
- **SSRF** (`app/security/ssrf.py`): http/https only; no URL credentials; blocks loopback, private, link-local (incl. cloud metadata), CGNAT, multicast, reserved, IPv4-mapped IPv6,
  `localhost`/`.internal`/`.local`; resolves DNS and rejects if *any* record is non-public; decimal/octal IP forms are caught by resolution; **re-validated on every delivery attempt**;
  redirects are not followed. Residual risk: DNS rebinding between check and connect (mitigate with an egress proxy / connect-by-IP).
- **Secrets in logs**: JSON formatter redacts sensitive field names and key patterns; payloads are never logged; response bodies are never stored. Test: `tests/unit/test_logging_redaction.py`.
- **Tenant isolation**: all management queries filter by the JWT's account; `/tenants` lists all only for admins.
- **Known limitations**: signing secrets are stored in plaintext (needed to sign) — add envelope encryption/KMS; no login/API rate limiting; HS256 single secret, no rotation; demo defaults
  (`ALLOW_PRIVATE_DESTINATIONS=true`, dev secrets) are unsafe for production; CORS is a fixed allow-list.
