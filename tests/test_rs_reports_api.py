"""Focused tests for RSReportsAPI auth header behavior.

This module was written by GitHub Copilot.
"""

from __future__ import annotations

from typing import Any

from api.lib.RSReportsAPI import RSReportsAPI


class _FakeResponse:
    """Minimal fake response object for requests.post monkeypatching."""

    def __init__(self, payload: dict[str, Any], status_code: int = 200) -> None:
        """Store payload and HTTP status for a fake response."""
        self._payload = payload
        self.status_code = status_code

    def json(self) -> dict[str, Any]:
        """Return the stored JSON payload."""
        return self._payload


def test_run_query_uses_api_token_header_for_production_style_args(monkeypatch) -> None:
    """Verify production-style args send x-api-key when no access token exists."""
    captured: dict[str, Any] = {}

    def _fake_post(url: str, json: dict[str, Any], headers: dict[str, str], timeout: int) -> _FakeResponse:
        """Capture outbound request parameters for assertion."""
        captured["url"] = url
        captured["json"] = json
        captured["headers"] = headers
        captured["timeout"] = timeout
        return _FakeResponse({"data": {"ok": True}}, status_code=200)

    monkeypatch.setattr("api.lib.RSReportsAPI.requests.post", _fake_post)

    api_client = RSReportsAPI(stage="production", api_token="tok_123456")
    response = api_client.run_query("query Q { ok }", variables={"a": 1})

    assert response == {"data": {"ok": True}}
    assert captured["headers"]["x-api-key"] == "tok_123456"
    assert "authorization" not in captured["headers"]


def test_run_query_uses_access_token_header_when_available(monkeypatch) -> None:
    """Verify run_query sends a Bearer authorization header when access token is set."""
    captured: dict[str, Any] = {}

    def _fake_post(url: str, json: dict[str, Any], headers: dict[str, str], timeout: int) -> _FakeResponse:
        """Capture outbound request headers to validate auth precedence."""
        captured["url"] = url
        captured["json"] = json
        captured["headers"] = headers
        captured["timeout"] = timeout
        return _FakeResponse({"data": {"ok": True}}, status_code=200)

    monkeypatch.setattr("api.lib.RSReportsAPI.requests.post", _fake_post)

    api_client = RSReportsAPI(stage="production", api_token="tok_123456")
    api_client.access_token = "access_abcdef"
    response = api_client.run_query("query Q { ok }", variables={})

    assert response == {"data": {"ok": True}}
    assert captured["headers"]["authorization"] == "Bearer access_abcdef"
    assert "x-api-key" not in captured["headers"]
