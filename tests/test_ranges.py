import io
import random
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from common_crawl_bloom import BloomFilter
from common_crawl_bloom.build import build
from common_crawl_bloom.source import DownloadError, Downloader, HTTPRangeReader, consume_generated
from test_pipeline import FakeDownloader, Response


def test_remote_seek_read_and_limit():
    downloader = FakeDownloader({"file": b"0123456789"})
    with HTTPRangeReader("file", downloader, 4) as reader:
        assert reader.read(3) == b"012"
        assert reader.tell() == 3
        assert reader.seek(-2, io.SEEK_END) == 8
        assert reader.read(4) == b"89"
        assert reader.read(4) == b""
        reader.seek(0)
        with pytest.raises(DownloadError, match="max-range"):
            reader.read()
        with pytest.raises(ValueError):
            reader.seek(-1)
        target = bytearray(2)
        assert reader.readinto(target) == 2 and target == b"01"
        assert reader.downloaded_bytes == 7
    with pytest.raises(ValueError):
        reader.read(1)


@pytest.mark.parametrize("status,content_range", [(200, None), (206, "bytes 1-3/10")])
def test_range_requests_fail_closed(status, content_range):
    response = Response(b"012", 3)
    response.status = status
    if content_range:
        response.headers["Content-Range"] = content_range
    with patch("common_crawl_bloom.source.urlopen", return_value=response):
        with pytest.raises(DownloadError, match="honor byte range"):
            Downloader().get("https://example.org/", 3, (0, 2))


def test_valid_range():
    response = Response(b"012", 3)
    response.status = 206
    response.headers["Content-Range"] = "bytes 0-2/10"
    with patch("common_crawl_bloom.source.urlopen", return_value=response) as opened:
        with Downloader().get("https://example.org/", 3, (0, 2)) as result:
            assert result.read() == b"012"
    assert opened.call_args.args[0].get_header("Range") == "bytes=0-2"


def test_only_selected_columns_downloaded():
    rng = random.Random(123)
    table = pa.table({"url": [f"https://example.org/{i}" for i in range(100)],
                      "fetch_status": [200] * 100,
                      "unused": [rng.randbytes(10000) for _ in range(100)]})
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="NONE", row_group_size=50)
    data = buffer.getvalue()
    bloom = BloomFilter(200)
    stats = build(["file"], bloom, FakeDownloader({"file": data}), 65536, batch_size=11)
    assert stats["accepted_urls"] == 100
    assert stats["downloaded_bytes"] < len(data) / 5
    assert all(f"https://example.org/{i}" in bloom for i in range(100))


def test_worker_error_is_propagated_and_generator_closed():
    closed = []

    def generate():
        try:
            yield 1
            raise RuntimeError("failed download")
        finally:
            closed.append(True)

    with pytest.raises(RuntimeError, match="failed download"):
        consume_generated(generate(), lambda item: None)
    assert closed == [True]


def test_consumer_error_closes_generator():
    closed = []

    def generate():
        try:
            yield from range(100)
        finally:
            closed.append(True)

    def consume(item):
        raise ValueError("insertion failed")

    with pytest.raises(ValueError, match="insertion failed"):
        consume_generated(generate(), consume)
    assert closed == [True]
