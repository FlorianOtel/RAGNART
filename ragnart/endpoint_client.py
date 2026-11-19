"""
Client for the RAGNART ingestion API (v0.3 session B1): the endpoint host's side of ragnart/ingest_api.py.

An endpoint host (the operator's Mac, in B1) submits its items to the hub through the ingestion API.
HubClient has one method per API endpoint. Each method sends the request the API expects and returns
the answer as a dict (or a list, for items). Every failure the hub reports, except the 409 refusal of a
project publish, raises HubError.

Dependencies: httpx only. This module loads no docling, qdrant_client or embedder. The import contract
is enforced by tests/test_ingestion_side_imports.py.

The secret: HubClient reads it once, from a file, when it is made. The secret is sent only as the Bearer
header of each request. It is never logged, and it is not part of repr() or of any exception message.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx


class HubError(Exception):
    """
    The hub answered with a failure status.

    status_code: the HTTP status the hub returned.
    detail:      the hub's error detail (the 'detail' field of its JSON answer, or the raw body text).
                 The hub never echoes a source secret, so neither field can carry one.
    """

    def __init__(self, status_code: int, detail: Any) -> None:
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"hub answered HTTP {status_code}: {detail}")


def _error_detail(response: httpx.Response) -> Any:
    """
    Return the detail of a failed answer: the JSON 'detail' field when there is one, else the body.

    A JSON body without 'detail' (the 409 refusal of a project publish, for example) is returned whole.
    A body that is not JSON is returned as text.
    """
    try:
        body = response.json()
    except ValueError:
        return response.text
    if isinstance(body, dict) and "detail" in body:
        return body["detail"]
    return body


def _json_or_raise(response: httpx.Response) -> Any:
    """Return the JSON body of a successful answer, or raise HubError for any other status."""
    if response.is_success:
        return response.json()
    raise HubError(response.status_code, _error_detail(response))


class HubClient:
    """
    One endpoint source's connection to the hub's ingestion API.

    Args:
        base_url:    The API's base URL, e.g. 'http://<hub>:8101'.
        secret_file: A file holding the source's secret. Surrounding whitespace is stripped.
        timeout:     Seconds to wait for one request.
        http_client: Optional httpx.Client to send through instead of a new one. Tests pass a client
                     bound to the API app; in that case base_url is kept for repr() only.

    Raises:
        ValueError: the secret file is empty. The message names the file, never its content.
    """

    def __init__(
        self,
        base_url: str,
        secret_file: str | Path,
        timeout: float = 60,
        *,
        http_client: httpx.Client | None = None,
    ) -> None:
        # Read once. A later change to the file does not affect this client.
        secret = Path(secret_file).read_text(encoding="utf-8").strip()
        if not secret:
            raise ValueError(f"secret file {secret_file} is empty")
        self._secret = secret
        self._base_url = base_url
        if http_client is not None:
            self._http = http_client
        else:
            self._http = httpx.Client(base_url=base_url, timeout=timeout)

    def __repr__(self) -> str:
        """Name the hub only. The secret is never shown."""
        return f"HubClient(base_url={self._base_url!r})"

    def close(self) -> None:
        """Close the underlying HTTP connection pool."""
        self._http.close()

    # ------------------------------------------------------------------
    # Requests

    def _send(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        """Send one request with this source's Bearer secret. The answer is returned unchecked."""
        headers = {"Authorization": f"Bearer {self._secret}"}
        return self._http.request(method, path, headers=headers, **kwargs)

    # ------------------------------------------------------------------
    # Writes: each one returns the API's 202 answer, {request_id, status}

    def upsert_bytes(self, path: str, key: str, file_type: str, modified_at: str | None = None) -> dict:
        """
        Submit one local file's bytes as an upsert.

        The file is opened here and streamed by httpx while it is sent, so a large file is never read
        into memory whole. file_type is the declared type (the API checks it against its include list).
        modified_at, when given, must be an ISO-8601 string with a UTC offset.
        """
        data = {"key": key, "file_type": file_type}
        if modified_at is not None:
            data["modified_at"] = modified_at
        with open(path, "rb") as stream:
            response = self._send(
                "POST", "/v1/upsert/bytes",
                data=data,
                files={"file": (Path(path).name, stream)},
            )
        return _json_or_raise(response)

    def upsert_text(
        self,
        key: str,
        text: str,
        metadata: dict | None = None,
        modified_at: str | None = None,
    ) -> dict:
        """Submit one item's rendered text as an upsert (claude_code sources). metadata holds the chat fields."""
        body = {"key": key, "text": text, "metadata": metadata, "modified_at": modified_at}
        return _json_or_raise(self._send("POST", "/v1/upsert/text", json=body))

    def delete(self, key: str) -> dict:
        """Ask the hub to remove one item of this source."""
        return _json_or_raise(self._send("POST", "/v1/delete", json={"key": key}))

    def change_key(self, key: str, new_key: str) -> dict:
        """Ask the hub to move one item to a new key. Its points are kept, nothing is re-embedded."""
        return _json_or_raise(self._send("POST", "/v1/change-key", json={"key": key, "new_key": new_key}))

    def change_metadata(self, key: str, owner: str) -> dict:
        """Ask the hub to set the owner of one item of this source. Only owner may change in B1."""
        body = {"key": key, "metadata": {"owner": owner}}
        return _json_or_raise(self._send("POST", "/v1/change-metadata", json=body))

    def publish_project(self, project_id: str, name: str, path: str | None = None) -> dict:
        """
        Ask the hub to publish a ragnart project from this source.

        In B1 the hub always refuses, with 409. The refusal is an answer, not a failure, so its body
        ({'refused': True, 'reason': ...}) is returned rather than raised.
        """
        body = {"id": project_id, "name": name, "path": path}
        response = self._send("POST", "/v1/projects", json=body)
        if response.status_code == 409:
            return response.json()
        return _json_or_raise(response)

    # ------------------------------------------------------------------
    # Reads

    def status(self, request_id: str) -> dict:
        """Return how far one request has got, with its admission result and error when it has them."""
        return _json_or_raise(self._send("GET", f"/v1/status/{quote(request_id, safe='')}"))

    def items(self) -> list[dict]:
        """Return this source's items as the hub sees them: key, chunk_count, content_hash, pipeline_version."""
        return _json_or_raise(self._send("GET", "/v1/items"))["items"]

    def info(self) -> dict:
        """Return the pipeline version, the accepted file types and the admission limits."""
        return _json_or_raise(self._send("GET", "/v1/info"))
