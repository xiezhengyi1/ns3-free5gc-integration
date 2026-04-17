"""Optional HTTP sink for external ingestion endpoints."""

from __future__ import annotations

import json
from urllib import request
from urllib.error import HTTPError, URLError

from bridge.common.schema import TickSnapshot


class HttpIngestionClient:
    def __init__(self, ingestion_url: str, timeout: float = 10.0) -> None:
        self.ingestion_url = ingestion_url
        self.timeout = timeout

    def post_snapshot(self, snapshot: TickSnapshot) -> dict[str, object]:
        payload = json.dumps(snapshot.to_dict(), ensure_ascii=False).encode("utf-8")
        http_request = request.Request(
            self.ingestion_url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with request.urlopen(http_request, timeout=self.timeout) as response:
                body = response.read().decode("utf-8")
                if not body:
                    return {"status": response.status}
                try:
                    return json.loads(body)
                except json.JSONDecodeError:
                    return {"status": response.status, "body": body}
        except HTTPError as exc:
            raise RuntimeError(f"HTTP ingestion failed with status {exc.code}") from exc
        except URLError as exc:
            raise RuntimeError(f"HTTP ingestion failed: {exc.reason}") from exc