"""Structured JSON logging with trace-id context and secret redaction."""
from __future__ import annotations

import contextvars
import json
import logging
import re
import sys
from datetime import datetime, timezone

_ctx: contextvars.ContextVar[dict] = contextvars.ContextVar("log_ctx", default={})
_SENSITIVE_KEYS = re.compile(r"(secret|password|authorization|api[_-]?key|token|payload|signature)", re.I)
_KEY_PATTERN = re.compile(r"hr_[0-9a-f]{8}_[A-Za-z0-9_\-]{10,}|whsec_[A-Za-z0-9_\-]{10,}")


def bind(**kw) -> None:
    _ctx.set({**_ctx.get(), **{k: str(v) for k, v in kw.items() if v is not None}})


def clear() -> None:
    _ctx.set({})


def redact(text: str) -> str:
    return _KEY_PATTERN.sub("[REDACTED]", text)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        out = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": redact(record.getMessage()),
            **_ctx.get(),
        }
        for k, v in getattr(record, "fields", {}).items():
            out[k] = "[REDACTED]" if _SENSITIVE_KEYS.search(k) else v
        if record.exc_info:
            out["exc"] = redact(self.formatException(record.exc_info))
        return json.dumps(out, default=str)


def setup_logging(level: str = "INFO") -> None:
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [h]
    root.setLevel(level)
    logging.getLogger("httpx").setLevel(logging.WARNING)


def log(logger: logging.Logger, level: int, msg: str, **fields) -> None:
    logger.log(level, msg, extra={"fields": fields})
