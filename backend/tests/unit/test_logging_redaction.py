import json
import logging

from app.core import logging as hlog


def _emit(**fields):
    rec = logging.LogRecord("t", logging.INFO, __file__, 1, "key hr_deadbeef_AbCdEfGhIjKlMnOp used whsec_ABCDEFGHIJKLMNOP", None, None)
    rec.fields = fields
    return json.loads(hlog.JsonFormatter().format(rec))


def test_secrets_in_message_and_sensitive_fields_are_redacted():
    out = _emit(signing_secret="whsec_real", api_key="hr_x", payload={"card": "4242"}, delivery_id="d1")
    assert "AbCdEfGhIjKlMnOp" not in out["msg"] and "ABCDEFGHIJKLMNOP" not in out["msg"]
    assert out["signing_secret"] == out["api_key"] == out["payload"] == "[REDACTED]"
    assert out["delivery_id"] == "d1"


def test_trace_context_is_attached():
    hlog.bind(request_id="r1", event_id="e1", delivery_id="d1", attempt=2)
    out = _emit()
    assert (out["request_id"], out["event_id"], out["delivery_id"], out["attempt"]) == ("r1", "e1", "d1", "2")
    hlog.clear()
