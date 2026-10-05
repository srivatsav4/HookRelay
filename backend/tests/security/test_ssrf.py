import pytest

from app.security.ssrf import SSRFError, validate_destination_url as v

PUBLIC = lambda host, port: ["93.184.216.34"]  # noqa: E731


@pytest.mark.parametrize("url", [
    "http://127.0.0.1/", "http://127.1.2.3:8080/x", "http://localhost/", "http://LOCALHOST./", "http://foo.localhost/",
    "http://[::1]/", "http://[::ffff:127.0.0.1]/", "http://169.254.169.254/latest/meta-data/", "http://10.0.0.1/",
    "http://172.16.5.4/", "http://172.31.255.255/", "http://192.168.0.10/", "http://0.0.0.0/", "http://100.64.0.1/",
    "http://[fe80::1]/", "http://[fc00::1]/", "http://224.0.0.1/", "http://metadata.google.internal/",
    "http://db.internal/", "http://printer.local/"])
def test_dangerous_destinations_rejected(url):
    with pytest.raises(SSRFError):
        v(url, resolver=PUBLIC)


@pytest.mark.parametrize("url", ["file:///etc/passwd", "ftp://example.com/", "gopher://x/", "javascript:alert(1)", "//example.com", "http:///nohost",
                                 "http://user:pw@example.com/", "http://example.com:99999/"])
def test_bad_schemes_and_shapes_rejected(url):
    with pytest.raises(SSRFError):
        v(url, resolver=PUBLIC)


def test_hostname_resolving_to_private_ip_rejected_dns_rebinding_style():
    with pytest.raises(SSRFError):
        v("http://innocent.example.com/", resolver=lambda h, p: ["93.184.216.34", "10.0.0.7"])  # any bad A record blocks
    with pytest.raises(SSRFError):
        v("http://2130706433/", resolver=lambda h, p: ["127.0.0.1"])  # decimal-encoded loopback


def test_unresolvable_host_rejected():
    def boom(h, p):
        raise OSError("nxdomain")
    with pytest.raises(SSRFError):
        v("http://nope.example.com/", resolver=boom)


def test_public_destination_allowed_and_https_policy():
    assert v("https://hooks.example.com/in", resolver=PUBLIC)
    assert v("http://hooks.example.com/in", resolver=PUBLIC)
    with pytest.raises(SSRFError):
        v("http://hooks.example.com/in", require_https=True, resolver=PUBLIC)


def test_allow_private_is_demo_only_but_still_checks_scheme_and_credentials():
    assert v("http://mock-receiver:9000/hook/x", allow_private=True)
    with pytest.raises(SSRFError):
        v("file:///etc/passwd", allow_private=True)
    with pytest.raises(SSRFError):
        v("http://u:p@mock-receiver/", allow_private=True)
