"""Common Crawl manifests and bounded in-memory HTTP downloads."""

from concurrent.futures import ThreadPoolExecutor
from email.utils import parsedate_to_datetime
import gzip
import http.client
import io
import json
import math
import random
import re
from threading import Lock
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

DATA_URL = "https://data.commoncrawl.org/"
COLLECTIONS_URL = "https://index.commoncrawl.org/collinfo.json"
CRAWL_ID = re.compile(r"CC-MAIN-\d{4}-\d{2}\Z")


class DownloadError(RuntimeError):
    pass


class Downloader:
    def __init__(self, timeout: float = 60, retries: int = 10, request_interval: float = 1):
        if not math.isfinite(timeout) or timeout <= 0 or retries < 0:
            raise ValueError("timeout must be finite and positive and retries nonnegative")
        if not math.isfinite(request_interval) or request_interval < 0:
            raise ValueError("request interval must be finite and nonnegative")
        self.timeout = timeout
        self.retries = retries
        self.request_interval = request_interval
        self._last_request = None
        self._pacing_lock = Lock()

    def _pace(self):
        # Share pacing across GETs, HEADs, and retries on this downloader.
        with self._pacing_lock:
            if self._last_request is not None:
                delay = self._last_request + self.request_interval - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
            self._last_request = time.monotonic()

    def _retry(self, exc, attempt, message):
        retry_after = 0
        if isinstance(exc, HTTPError):
            try:
                value = (exc.headers.get("Retry-After") or "") if exc.headers else ""
                if value.strip().isdigit():
                    retry_after = float(value)
                elif value:
                    retry_after = max(0, parsedate_to_datetime(value).timestamp() - time.time())
            except (ValueError, TypeError, OverflowError):
                pass
            finally:
                exc.close()
            if exc.code not in (408, 429, 500, 502, 503, 504):
                raise DownloadError(message) from exc
        if attempt == self.retries:
            raise DownloadError(message) from exc
        if not math.isfinite(retry_after):
            retry_after = 0
        base = min(2 ** min(attempt, 6), 48)
        delay = min(base + random.uniform(0, base / 4), 60)
        # The server's requested delay takes precedence over our backoff cap.
        time.sleep(max(delay, retry_after))

    def get(self, url: str, limit: int, byte_range: tuple[int, int] | None = None) -> io.BytesIO:
        """Return a RAM buffer, retrying incomplete transfers from scratch."""
        for attempt in range(self.retries + 1):
            buffer = io.BytesIO()
            try:
                headers = {"User-Agent": "common-crawl-bloom/0.1", "Accept-Encoding": "identity"}
                if byte_range is not None:
                    headers["Range"] = f"bytes={byte_range[0]}-{byte_range[1]}"
                request = Request(url, headers=headers)
                self._pace()
                with urlopen(request, timeout=self.timeout) as response:
                    if byte_range is not None:
                        content_range = response.headers.get("Content-Range", "")
                        expected_prefix = f"bytes {byte_range[0]}-{byte_range[1]}/"
                        if response.status != 206 or not content_range.startswith(expected_prefix):
                            raise DownloadError(f"{url}: server did not honor byte range")
                    length = response.headers.get("Content-Length")
                    expected = int(length) if length is not None else None
                    if byte_range is not None:
                        expected = byte_range[1] - byte_range[0] + 1
                    if expected is not None and expected > limit:
                        raise DownloadError(f"{url}: {expected} bytes exceeds RAM read limit {limit}")
                    while block := response.read(min(1024 * 1024, limit - buffer.tell() + 1)):
                        if buffer.tell() + len(block) > limit:
                            raise DownloadError(f"{url}: download exceeds limit {limit}")
                        buffer.write(block)
                    if expected is not None and buffer.tell() != expected:
                        raise http.client.IncompleteRead(b"", expected - buffer.tell())
                buffer.seek(0)
                return buffer
            except (HTTPError, URLError, TimeoutError, ConnectionError,
                    http.client.HTTPException) as exc:
                buffer.close()
                self._retry(exc, attempt, f"download failed: {url}: {exc}")
            except BaseException:
                buffer.close()
                raise
        raise AssertionError("unreachable")

    def size(self, url: str) -> int:
        for attempt in range(self.retries + 1):
            try:
                self._pace()
                with urlopen(Request(url, method="HEAD", headers={
                        "User-Agent": "common-crawl-bloom/0.1", "Accept-Encoding": "identity"}),
                        timeout=self.timeout) as response:
                    size = int(response.headers.get("Content-Length", "0"))
                    if size <= 0:
                        raise DownloadError(f"{url}: missing object size")
                    return size
            except (HTTPError, URLError, TimeoutError, ConnectionError,
                    http.client.HTTPException) as exc:
                self._retry(exc, attempt, f"could not get size: {url}: {exc}")
        raise AssertionError("unreachable")


class HTTPRangeReader(io.RawIOBase):
    """Seekable remote file with no disk cache and a hard per-read RAM limit."""

    def __init__(self, url: str, downloader: Downloader, max_range_bytes: int):
        super().__init__()
        self.url = url
        self.downloader = downloader
        self.max_range_bytes = max_range_bytes
        self.length = downloader.size(url)
        self.position = 0
        self.downloaded_bytes = 0

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self.position

    def seek(self, offset, whence=io.SEEK_SET):
        if whence not in (io.SEEK_SET, io.SEEK_CUR, io.SEEK_END):
            raise ValueError("invalid seek origin")
        position = offset + (0 if whence == io.SEEK_SET else
                             self.position if whence == io.SEEK_CUR else self.length)
        if position < 0:
            raise ValueError("negative seek position")
        self.position = position
        return position

    def read(self, size=-1):
        if self.closed:
            raise ValueError("read of closed remote file")
        remaining = max(0, self.length - self.position)
        size = remaining if size is None or size < 0 else min(size, remaining)
        if size > self.max_range_bytes:
            raise DownloadError(f"Parquet read needs {size} bytes; increase --max-range-mib")
        if size == 0:
            return b""
        with self.downloader.get(self.url, size, (self.position, self.position + size - 1)) as buffer:
            data = buffer.getvalue()
        self.position += size
        self.downloaded_bytes += size
        return data

    def readinto(self, buffer):
        data = self.read(len(buffer))
        buffer[:len(data)] = data
        return len(data)


def consume_generated(iterator, consume):
    """Prepare one next item on a worker while consuming the current item."""
    sentinel = object()
    iterator = iter(iterator)
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="cc-columns") as pool:
        future = pool.submit(next, iterator, sentinel)
        try:
            while True:
                item = future.result()
                if item is sentinel:
                    break
                future = pool.submit(next, iterator, sentinel)
                consume(item)
                del item
        finally:
            future.cancel()
            # Wait before closing: the generator may still be executing I/O.
            pool.shutdown(wait=True, cancel_futures=True)
            if hasattr(iterator, "close"):
                iterator.close()


def collections(downloader: Downloader) -> list[dict]:
    with downloader.get(COLLECTIONS_URL, 4 * 1024 * 1024) as buf:
        records = json.load(buf)
    return sorted((r for r in records if CRAWL_ID.fullmatch(r["id"])),
                  key=lambda r: r["id"], reverse=True)


def shard_urls(crawl: str, downloader: Downloader) -> list[str]:
    if not CRAWL_ID.fullmatch(crawl):
        raise ValueError(f"invalid crawl ID: {crawl}")
    manifest = f"{DATA_URL}crawl-data/{crawl}/cc-index-table.paths.gz"
    prefix = f"cc-index/table/cc-main/warc/crawl={crawl}/subset=warc/"
    with downloader.get(manifest, 4 * 1024 * 1024) as buf:
        with gzip.GzipFile(fileobj=buf) as decompressed:
            raw = decompressed.read(16 * 1024 * 1024 + 1)
    if len(raw) > 16 * 1024 * 1024:
        raise ValueError("manifest is too large")
    keys = list(dict.fromkeys(raw.decode("utf-8").splitlines()))
    selected = [key for key in keys if key.startswith(prefix) and key.endswith(".parquet")]
    if not selected or any(".." in key or "?" in key or "#" in key for key in selected):
        raise ValueError(f"no valid WARC Parquet shards in manifest for {crawl}")
    return [DATA_URL + key for key in selected]
