"""Concurrent download with httpx (mocked)."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
from pytest_httpx import HTTPXMock

from src.ingestion.gharchive import download_many


def _payload() -> bytes:
    return b"\x1f\x8b\x08\x00fake-gzip-bytes"


def test_download_many_writes_files(tmp_path: Path, httpx_mock: HTTPXMock) -> None:
    urls = [
        "https://data.gharchive.org/2024-01-01-0.json.gz",
        "https://data.gharchive.org/2024-01-01-1.json.gz",
    ]
    payload = _payload()
    for url in urls:
        httpx_mock.add_response(url=url, content=payload)

    paths = download_many(urls, tmp_path, concurrency=2, retries=1)

    assert sorted(p.name for p in paths) == [
        "2024-01-01-0.json.gz",
        "2024-01-01-1.json.gz",
    ]
    for path in paths:
        assert path.read_bytes() == payload


def test_download_many_empty_returns_empty(tmp_path: Path) -> None:
    assert download_many([], tmp_path) == []


def test_download_many_skips_existing_files(tmp_path: Path, httpx_mock: HTTPXMock) -> None:
    url = "https://data.gharchive.org/2024-01-01-0.json.gz"
    cached = tmp_path / "2024-01-01-0.json.gz"
    cached.write_bytes(b"already-here")

    paths = download_many([url], tmp_path, concurrency=1, retries=1)

    assert paths == [cached]
    assert cached.read_bytes() == b"already-here"
    assert httpx_mock.get_requests() == []


def test_download_many_retries_on_transient_failure(tmp_path: Path, httpx_mock: HTTPXMock) -> None:
    url = "https://data.gharchive.org/2024-01-01-0.json.gz"
    payload = _payload()
    httpx_mock.add_response(url=url, status_code=503)
    httpx_mock.add_response(url=url, content=payload)

    paths = download_many([url], tmp_path, concurrency=1, retries=2, timeout_s=5.0)

    assert paths[0].read_bytes() == payload
    assert len(httpx_mock.get_requests()) == 2


def test_download_many_raises_after_exhausted_retries(
    tmp_path: Path, httpx_mock: HTTPXMock
) -> None:
    url = "https://data.gharchive.org/2024-01-01-0.json.gz"
    httpx_mock.add_response(url=url, status_code=500)
    httpx_mock.add_response(url=url, status_code=500)

    with pytest.raises(httpx.HTTPStatusError):
        download_many([url], tmp_path, concurrency=1, retries=2, timeout_s=5.0)

    # Partial files must not leak.
    assert list(tmp_path.glob("*.part")) == []
    assert list(tmp_path.glob("*.json.gz")) == []
