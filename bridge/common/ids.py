"""Identifier helpers used across orchestration and storage."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4


def safe_name(value: str) -> str:
    sanitized = []
    for char in value.strip().lower():
        if char.isalnum():
            sanitized.append(char)
        elif char in {"-", "_"}:
            sanitized.append(char)
        else:
            sanitized.append("-")
    compact = "".join(sanitized).strip("-")
    return compact or "unnamed"


def generate_run_id(prefix: str = "run", now: datetime | None = None) -> str:
    current = now or datetime.now(timezone.utc)
    stamp = current.strftime("%Y%m%d%H%M%S")
    return f"{safe_name(prefix)}-{stamp}-{uuid4().hex[:8]}"