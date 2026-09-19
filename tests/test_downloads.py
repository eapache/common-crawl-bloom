"""Exercise request scheduling without real network traffic or waiting."""
from email.utils import formatdate
from urllib.error import HTTPError

import pytest

from common_crawl_bloom.cli import main
from common_crawl_bloom.source import DownloadError, Downloader
from test_pipeline import Response


@pytest.fixture
def clock(monkeypatch):
    class Clock:
        now = 1000.0

        def sleep(self, seconds):
            self.now += seconds

    clock = Clock()
    monkeypatch.setattr("common_crawl_bloom.source.time.monotonic", lambda: clock.now)
    monkeypatch.setattr("common_crawl_bloom.source.time.time", lambda: clock.now)
    monkeypatch.setattr("common_crawl_bloom.source.time.sleep", clock.sleep)
    return clock


def fetch(downloader, method):
    if method == "HEAD":
        assert downloader.size("https://example.org/") == 3
    else:
        with downloader.get("https://example.org/", 3, (0, 2)) as result:
            assert result.read() == b"abc"


def response():
    result = Response(b"abc", 3)
    result.status = 206
    result.headers["Content-Range"] = "bytes 0-2/3"
    return result


@pytest.mark.parametrize("interval", [0, 1, 2.5])
def test_pacing_shared_by_head_and_get(monkeypatch, clock, interval):
    starts = []

    def open_request(request, **kwargs):
        starts.append(clock.now)
        return response()

    monkeypatch.setattr("common_crawl_bloom.source.urlopen", open_request)
    downloader = Downloader(request_interval=interval)
    fetch(downloader, "HEAD")
    fetch(downloader, "GET")
    clock.sleep(5)
    fetch(downloader, "GET")
    assert starts == [1000, 1000 + interval, 1005 + interval]


@pytest.mark.parametrize("method", ["HEAD", "GET"])
@pytest.mark.parametrize("header,wait", [
    ("120", 120), (formatdate(1120, usegmt=True), 120),
    (formatdate(900, usegmt=True), 1.25), ("invalid", 1.25),
    ("-5", 1.25), (None, 1.25),
])
def test_retry_after_and_jitter(monkeypatch, clock, method, header, wait):
    starts = []
    error = HTTPError("url", 429, "slow down", {"Retry-After": header}, Response(b"error"))

    def open_request(request, **kwargs):
        starts.append(clock.now)
        if len(starts) == 1:
            raise error
        return response()

    monkeypatch.setattr("common_crawl_bloom.source.urlopen", open_request)
    monkeypatch.setattr("common_crawl_bloom.source.random.uniform", lambda low, high: high)
    fetch(Downloader(), method)
    assert starts == [1000, 1000 + wait]
    assert error.fp.closed


@pytest.mark.parametrize("method", ["HEAD", "GET"])
@pytest.mark.parametrize("status,retries,attempts", [(503, 10, 11), (429, 0, 1), (404, 10, 1)])
def test_retry_budget_and_cap(monkeypatch, clock, method, status, retries, attempts):
    starts = []

    def open_request(request, **kwargs):
        starts.append(clock.now)
        raise HTTPError("url", status, "failure", {}, None)

    monkeypatch.setattr("common_crawl_bloom.source.urlopen", open_request)
    monkeypatch.setattr("common_crawl_bloom.source.random.uniform", lambda low, high: high)
    with pytest.raises(DownloadError):
        fetch(Downloader(retries=retries), method)
    assert len(starts) == attempts
    if attempts > 1:
        assert [b - a for a, b in zip(starts, starts[1:])] == [
            1.25, 2.5, 5, 10, 20, 40, 60, 60, 60, 60]
        assert clock.now == starts[-1]  # No sleep after the final failure.


@pytest.mark.parametrize("value", [-1, float("nan"), float("inf")])
def test_invalid_interval(value):
    with pytest.raises(ValueError, match="request interval"):
        Downloader(request_interval=value)


@pytest.mark.parametrize("command", ["crawls", "plan", "build"])
def test_cli_passes_download_options(monkeypatch, tmp_path, command):
    from unittest.mock import patch

    args = ["cc-bloom", command, "--request-interval", "2.5", "--retries", "20", "--timeout", "30"]
    if command != "crawls":
        args += ["--latest", "1"]
    if command == "build":
        args += ["--output", str(tmp_path / "out.bloom")]
    monkeypatch.setattr("sys.argv", args)
    with patch("common_crawl_bloom.cli.Downloader") as downloader, \
            patch("common_crawl_bloom.cli.collections", side_effect=RuntimeError("stop before download")):
        assert main() == 2
    downloader.assert_called_once_with(30, 20, 2.5)
