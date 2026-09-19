import gzip
import io
import json
from threading import Event
from unittest.mock import patch
from urllib.error import HTTPError

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from common_crawl_bloom import BloomFilter
from common_crawl_bloom.build import build
from common_crawl_bloom.cli import main
from common_crawl_bloom.source import (DATA_URL, DownloadError, Downloader,
                                      collections, consume_generated, shard_urls)


def parquet_bytes(urls, statuses):
    buffer = io.BytesIO()
    pq.write_table(pa.table({"url": urls, "fetch_status": statuses}), buffer, row_group_size=2)
    return buffer.getvalue()


class FakeDownloader:
    def __init__(self, responses):
        self.responses = responses
        self.buffers = []

    def size(self, url):
        return len(self.responses[url])

    def get(self, url, limit, byte_range=None):
        data = self.responses[url]
        if byte_range is not None:
            start, end = byte_range
            data = data[start:end + 1]
        assert len(data) <= limit
        buffer = io.BytesIO(data)
        self.buffers.append(buffer)
        return buffer


def test_build_union_and_status_policy():
    a, b = "https://example.org/a", "https://example.org/b?x=1"
    downloader = FakeDownloader({
        "a": parquet_bytes([a, "https://example.org/redirect", "https://example.org/failed",
                            "https://example.org/missing"], [200, 301, 500, None]),
        "b": parquet_bytes([a, b, "ftp://example.org/", None], [200, 204, 200, 200])})
    bloom = BloomFilter(100)
    reports = []
    stats = build(["a", "b"], bloom, downloader, 10000, batch_size=1, progress=reports.append)
    assert a in bloom and b in bloom
    assert "https://example.org/redirect" not in bloom
    assert stats["rows"] == 8
    assert stats["accepted_urls"] == 3
    assert stats["rejected_urls"] == 2
    assert stats["shards"] == 2 and len(reports) == 2
    assert all(buffer.closed for buffer in downloader.buffers)


def test_prefetch_overlaps_and_is_bounded():
    started_second = Event()
    calls = []

    def generate():
        for name in ["first", "second", "third"]:
            calls.append(name)
            if name == "second":
                started_second.set()
            yield name

    def consume(item):
        if item == "first":
            assert started_second.wait(2)
            assert calls == ["first", "second"]

    consume_generated(generate(), consume)
    assert calls == ["first", "second", "third"]


def test_processing_error_closes_buffers():
    downloader = FakeDownloader({"a": b"not parquet", "b": b"another shard"})
    with pytest.raises(pa.ArrowInvalid):
        build(["a", "b"], BloomFilter(10), downloader, 100)
    assert all(buffer.closed for buffer in downloader.buffers)


def test_missing_columns_and_empty_input():
    buffer = io.BytesIO()
    pq.write_table(pa.table({"url": ["https://example.org/"]}), buffer)
    with pytest.raises(ValueError, match="lacks"):
        build(["a"], BloomFilter(10), FakeDownloader({"a": buffer.getvalue()}), 10000)
    with pytest.raises(ValueError, match="no eligible"):
        build([], BloomFilter(10), FakeDownloader({}), 10000)


def test_manifest_filters_subset_and_deduplicates():
    crawl = "CC-MAIN-2025-33"
    base = f"cc-index/table/cc-main/warc/crawl={crawl}/"
    key = base + "subset=warc/part-00000.parquet"
    manifest = f"{key}\n{base}subset=robotstxt/other.parquet\n{key}\n"
    downloader = FakeDownloader({f"{DATA_URL}crawl-data/{crawl}/cc-index-table.paths.gz":
                                 gzip.compress(manifest.encode())})
    assert shard_urls(crawl, downloader) == [DATA_URL + key]
    with pytest.raises(ValueError):
        shard_urls("../../invalid", downloader)


def test_collections_sorted():
    from common_crawl_bloom.source import COLLECTIONS_URL
    downloader = FakeDownloader({COLLECTIONS_URL: json.dumps([
        {"id": "CC-MAIN-2024-51"}, {"id": "other"}, {"id": "CC-MAIN-2025-33"}]).encode()})
    assert [r["id"] for r in collections(downloader)] == ["CC-MAIN-2025-33", "CC-MAIN-2024-51"]


class Response(io.BytesIO):
    def __init__(self, data, content_length=None):
        super().__init__(data)
        self.headers = {} if content_length is None else {"Content-Length": str(content_length)}


def test_download_retries_partial_transfer():
    responses = [Response(b"short", 10), Response(b"successful", 10)]
    with patch("common_crawl_bloom.source.urlopen", side_effect=responses) as opened, \
            patch("common_crawl_bloom.source.time.sleep"):
        with Downloader(retries=1).get("https://example.org/", 100) as result:
            assert result.read() == b"successful"
    assert opened.call_count == 2
    assert all(response.closed for response in responses)


@pytest.mark.parametrize("declared", [None, 101])
def test_download_size_bound(declared):
    with patch("common_crawl_bloom.source.urlopen", return_value=Response(b"x" * 101, declared)):
        with pytest.raises(DownloadError, match="exceeds"):
            Downloader().get("https://example.org/", 100)


def test_404_not_retried():
    with patch("common_crawl_bloom.source.urlopen", side_effect=HTTPError("url", 404, "missing", {}, None)) as opened:
        with pytest.raises(DownloadError):
            Downloader().get("https://example.org/", 100)
    assert opened.call_count == 1


def test_cli_build_and_check(tmp_path, capsys):
    output = tmp_path / "union.bloom"
    downloader = FakeDownloader({"a": parquet_bytes(["https://example.org/"], [200]),
                                 "b": parquet_bytes(["https://example.org/b"], [201])})
    args = ["cc-bloom", "build", "--crawl", "CC-MAIN-2025-33", "--crawl", "CC-MAIN-2025-30",
            "--expected-urls", "100", "--output", str(output)]
    with patch("sys.argv", args), patch("common_crawl_bloom.cli.Downloader", return_value=downloader), \
            patch("common_crawl_bloom.cli.shard_urls", side_effect=[["a"], ["b"]]):
        assert main() == 0
    loaded = BloomFilter.load(output)
    assert len(loaded.metadata["crawls"]) == 2
    assert loaded.metadata["partial"] is False
    with patch("sys.argv", ["cc-bloom", "check", str(output), "https://example.org/", "https://absent.org/"]):
        assert main() == 1
    assert '"possibly_present": false' in capsys.readouterr().out


def test_failed_build_never_publishes(tmp_path):
    output = tmp_path / "bad.bloom"
    with patch("sys.argv", ["cc-bloom", "build", "--crawl", "CC-MAIN-2025-33",
                            "--expected-urls", "100", "--output", str(output)]), \
            patch("common_crawl_bloom.cli.Downloader", return_value=FakeDownloader({"bad": b"invalid"})), \
            patch("common_crawl_bloom.cli.shard_urls", return_value=["bad"]):
        assert main() == 2
    assert not output.exists()


def test_partial_metadata_and_overfill_guard(tmp_path):
    output = tmp_path / "partial.bloom"
    downloader = FakeDownloader({"a": parquet_bytes(
        [f"https://example.org/{i}" for i in range(100)], [200] * 100)})
    args = ["cc-bloom", "build", "--crawl", "CC-MAIN-2025-33", "--expected-urls", "1",
            "--max-shards", "1", "--output", str(output)]
    with patch("sys.argv", args), patch("common_crawl_bloom.cli.Downloader", return_value=downloader), \
            patch("common_crawl_bloom.cli.shard_urls", return_value=["a", "b"]):
        assert main() == 2
        assert not output.exists()
    with patch("sys.argv", args + ["--allow-overfilled"]), \
            patch("common_crawl_bloom.cli.Downloader", return_value=downloader), \
            patch("common_crawl_bloom.cli.shard_urls", return_value=["a", "b"]):
        assert main() == 0
    loaded = BloomFilter.load(output)
    assert loaded.metadata["partial"] is True
    assert loaded.metadata["processed_shards"] == 1
    assert loaded.stats()["estimated_fpr"] > loaded.stats()["design_fpr"]
