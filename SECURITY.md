# Security policy

Report vulnerabilities privately to the maintainers (do not open a public issue). See `docs/security.md` for the threat
model and known limitations (signing secrets are stored in plaintext in PostgreSQL because HMAC signing needs them; use
disk/volume encryption or add envelope encryption before production use; there is no login rate limiting yet).
The demo stack sets `ALLOW_PRIVATE_DESTINATIONS=true` and uses development secrets: it is **not** production-safe as shipped.
