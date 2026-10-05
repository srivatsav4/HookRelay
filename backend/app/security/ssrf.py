"""SSRF guard for endpoint registration and for every delivery attempt. Pure stdlib."""
from __future__ import annotations

import asyncio
import ipaddress
import socket
from collections.abc import Callable
from urllib.parse import urlsplit


class SSRFError(ValueError):
    pass


BLOCKED_HOSTNAMES = {"localhost", "metadata.google.internal", "metadata"}
BLOCKED_SUFFIXES = (".localhost", ".internal", ".local")
EXTRA_BLOCKED_NETS = [ipaddress.ip_network(n) for n in (
    "0.0.0.0/8", "100.64.0.0/10", "169.254.0.0/16", "192.0.0.0/24", "198.18.0.0/15", "fd00:ec2::/32")]
Resolver = Callable[[str, int], list[str]]


def is_blocked_ip(value: str) -> bool:
    ip = ipaddress.ip_address(value.split("%")[0])
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped:
        ip = ip.ipv4_mapped
    return (ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast or ip.is_reserved
            or ip.is_unspecified or any(ip in n for n in EXTRA_BLOCKED_NETS))


def default_resolver(host: str, port: int) -> list[str]:
    return sorted({i[4][0] for i in socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)})


def validate_destination_url(url: str, *, allow_private: bool = False, require_https: bool = False,
                             resolver: Resolver = default_resolver) -> str:
    try:
        parts = urlsplit(url.strip())
        port = parts.port
    except ValueError as e:
        raise SSRFError("malformed URL") from e
    if parts.scheme not in ("http", "https"):
        raise SSRFError(f"scheme '{parts.scheme}' not allowed")
    if require_https and parts.scheme != "https":
        raise SSRFError("https required")
    host = (parts.hostname or "").rstrip(".").lower()
    if not host:
        raise SSRFError("missing host")
    if parts.username or parts.password:
        raise SSRFError("credentials in URL not allowed")
    if allow_private:  # demo/test only: scheme and credential checks still apply
        return url
    if host in BLOCKED_HOSTNAMES or host.endswith(BLOCKED_SUFFIXES):
        raise SSRFError(f"host '{host}' not allowed")
    try:
        ipaddress.ip_address(host)
        addrs = [host]
    except ValueError:
        try:
            addrs = resolver(host, port or (443 if parts.scheme == "https" else 80))
        except OSError as e:
            raise SSRFError(f"host '{host}' did not resolve") from e
    if not addrs:
        raise SSRFError("host did not resolve")
    for a in addrs:
        if is_blocked_ip(a):
            raise SSRFError(f"destination resolves to a non-public address ({a})")
    return url


async def validate_destination_url_async(url: str, **kw) -> str:
    return await asyncio.to_thread(validate_destination_url, url, **kw)
