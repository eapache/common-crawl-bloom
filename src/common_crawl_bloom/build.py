"""Read only the URL and response-status columns in bounded row batches."""

from dataclasses import asdict, dataclass
import time

import pyarrow.compute as pc
import pyarrow.parquet as pq

from .source import HTTPRangeReader, consume_generated


@dataclass
class BuildStats:
    shards: int = 0
    downloaded_bytes: int = 0
    rows: int = 0
    successful_rows: int = 0
    accepted_urls: int = 0
    rejected_urls: int = 0


def count_rows(urls, downloader, max_range_bytes=64 * 1024 * 1024, progress=None):
    """Read shard metadata for a conservative bound on distinct eligible URLs."""
    rows = downloaded_bytes = 0
    for count, url in enumerate(urls, 1):
        with HTTPRangeReader(url, downloader, max_range_bytes) as reader:
            with pq.ParquetFile(reader, pre_buffer=False, buffer_size=0) as parquet:
                if not {"url", "fetch_status"} <= set(parquet.schema_arrow.names):
                    raise ValueError("Parquet shard lacks url or fetch_status columns")
                rows += parquet.metadata.num_rows
            downloaded_bytes += reader.downloaded_bytes
        if progress:
            progress({"phase": "sizing", "shards": count, "rows": rows,
                      "downloaded_bytes": downloaded_bytes, "last_shard": url})
    if not rows:
        raise ValueError("no rows found in selected shards")
    return {"method": "parquet_row_upper_bound", "rows": rows,
            "downloaded_bytes": downloaded_bytes}


def build(urls, bloom, downloader, max_range_bytes: int = 64 * 1024 * 1024,
          batch_size: int = 65536, progress=None) -> dict:
    """Project columns over HTTPS ranges; prefetch one decoded batch in RAM."""
    if max_range_bytes <= 0 or batch_size <= 0:
        raise ValueError("range limit and batch size must be positive")
    stats = BuildStats()
    started = time.monotonic()

    def batches():
        for url in urls:
            with HTTPRangeReader(url, downloader, max_range_bytes) as reader:
                with pq.ParquetFile(reader, pre_buffer=False, buffer_size=0) as parquet:
                    if not {"url", "fetch_status"} <= set(parquet.schema_arrow.names):
                        raise ValueError("Parquet shard lacks url or fetch_status columns")
                    for batch in parquet.iter_batches(batch_size=batch_size,
                                                      columns=["url", "fetch_status"],
                                                      use_threads=False):
                        statuses = batch.column("fetch_status")
                        successful = pc.and_(pc.greater_equal(statuses, 200), pc.less(statuses, 300))
                        selected = pc.filter(batch.column("url"), successful).to_pylist()
                        yield ("batch", batch.num_rows, selected)
                    yield ("shard", reader.downloaded_bytes, url)

    def consume(item):
        kind, count, payload = item
        if kind == "batch":
            stats.rows += count
            stats.successful_rows += len(payload)
            for url in payload:
                if bloom.add(url):
                    stats.accepted_urls += 1
                else:
                    stats.rejected_urls += 1
        else:
            stats.shards += 1
            stats.downloaded_bytes += count
            if progress:
                progress({**asdict(stats), "elapsed_seconds": time.monotonic() - started,
                          "last_shard": payload, "estimated_fpr": bloom.stats()["estimated_fpr"]})

    consume_generated(batches(), consume)
    if not stats.accepted_urls:
        raise ValueError("no eligible URLs found; refusing to publish an empty filter")
    return {**asdict(stats), "elapsed_seconds": time.monotonic() - started}
