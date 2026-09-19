# common-crawl-bloom

Build a Bloom filter of exact HTTP(S) URLs observed with successful (2xx)
responses in one or more Common Crawl snapshots.

The builder reads URL and status columns from Common Crawl's
[Parquet index](https://commoncrawl.org/url-index) using HTTP byte ranges. It
prefetches the next batch while inserting the current batch into a RAM-resident
filter, then writes the completed filter sequentially. No downloaded shards or
disk caches are created, and no AWS credentials are needed.

**A match means “possibly observed,” not “safe.”** False positives and malicious
or stale crawled URLs make this unsuitable as the sole data-leakage boundary.

## Install and build

Requires Python 3.11+.

```sh
python3 -m venv .venv
.venv/bin/pip install -e '.[test]'
.venv/bin/cc-bloom crawls

# Preview filter size by reading metadata, without allocating the filter.
.venv/bin/cc-bloom plan --latest 1 --false-positive-rate 1e-6

# Try one shard from the latest crawl before committing to a full build.
.venv/bin/cc-bloom build --latest 1 --max-shards 1 \
  --output sample.bloom

# Combine two snapshots into one filter.
.venv/bin/cc-bloom build \
  --crawl CC-MAIN-2025-33 --crawl CC-MAIN-2025-30 \
  --false-positive-rate 1e-6 \
  --output urls.bloom
```

Builds automatically size the filter from the selected shards' Parquet row counts.
You do not need to estimate URL counts or overlap between crawls.
Use `--latest 3` for the newest three snapshots, or repeat `--crawl` for reproducible
selection. Crawl IDs use `CC-MAIN-YYYY-WW`, not calendar month numbers.

Even one shard can contain millions of URLs. `--max-shards` selects the first N
shards and records partial coverage in the artifact; remove it for a complete
build. More snapshots increase historical coverage, build time, and capacity needs.

## Size and memory

Set `--false-positive-rate` (default `1e-6`), or replace it with `--size-mib` for a
fixed bit-array budget. By default, a metadata pass sums the rows in the selected
shards before choosing the filter size and hash count. This is a conservative
upper bound on distinct eligible URLs: duplicate URLs, overlapping crawls, and
unsuccessful responses can leave unused capacity. The metadata pass does not
decode URL columns, but requires requests for every selected shard. Its download
cost is reported separately in the artifact's `sizing` metadata.

Use `plan --latest N` (or `plan --crawl ...`) with the same selection and sizing
options as your build to preview memory needs. `--max-shards` also limits the
sizing pass. With `--size-mib`, the allocation stays fixed and the program chooses
the hash count from the row bound; inspect the reported `design_fpr` for precision.

If you already have a distinct URL estimate, `--expected-urls N` overrides automatic
sizing and skips the metadata pass. Overlap does not increase filter occupancy.
`plan --expected-urls N` still works offline. Measuring the distinct union instead
of using a row bound would require an additional pass through the URL data.

For **1 billion distinct URLs**, approximate filter RAM is:

| False-positive probability | Bit-array RAM |
| --- | --- |
| `1e-3` | 1.67 GiB |
| `1e-6` | 3.35 GiB |
| `1e-9` | 5.02 GiB |

Leave additional RAM for Parquet decoding and prefetched batches; insufficient
physical memory can still cause swapping. `--batch-size` defaults to 65,536 rows.
`--max-range-mib` defaults to 64 and limits a single compressed-column/footer read,
**not total process memory**.

The filter records its estimated false-positive rate. If this exceeds the design
rate by more than 10%, publication fails unless `--allow-overfilled` is supplied.
This check happens at completion; it is not a guarantee of actual error rate.

## Inspect and query

```sh
.venv/bin/cc-bloom inspect urls.bloom
.venv/bin/cc-bloom check urls.bloom 'https://example.org/path?key=value'
```

`check` also accepts one URL per stdin line and emits JSON lines. Exit status is
0 when all URLs possibly match, 1 when any is absent/ineligible, and 2 on error.
Both commands verify the checksum and load the full filter into RAM.

```python
from common_crawl_bloom import BloomFilter

bloom = BloomFilter.load("urls.bloom")
possibly_observed = "https://example.org/path?key=value" in bloom
```

Keys preserve the exact ASCII URL, including scheme, path, query, parameter order,
and escaping. There is no normalization or domain-level matching. Credentials,
fragments, non-ASCII characters, and malformed URLs are rejected.

For strict fetch policies, confirm positive matches against an exact trusted set
or another explicit policy. Check the final transmitted URL and every redirect;
apply separate destination/IP and request-data restrictions. Never strip query
parameters before checking.

## Operational notes

- `crawls`, `plan`, and `build` accept `--request-interval SECONDS` (default `1`)
  to pace request starts, including HEAD requests and retries. Pacing is shared
  within one downloader, not across separate processes; `0` disables it.
- `--retries N` defaults to `10` retries per request. Transient failures use
  exponential backoff with jitter, starting at 1–1.25 seconds and capped at
  60 seconds. A valid `Retry-After` delay or HTTP date is honored even when it
  exceeds that cap. `--timeout` defaults to 60 seconds per socket operation,
  not a total deadline for all retries.
- For example, slow a build further with
  `cc-bloom build --latest 1 --request-interval 2 --retries 20 --output urls.bloom`.
  These defaults are conservative client settings, not a published Common Crawl
  quota; see their [download guidance](https://status.commoncrawl.org/).
- Progress is JSON on stderr after each shard; completion is JSON on stdout.
- Downloads retry transient failures. Failed builds do not publish a completed
  filter, and existing output files are never replaced.
- There is no checkpoint/resume yet. Interrupted builds must restart.
- A limited live test succeeded; full-crawl throughput has not been established.
  Benchmark a shard on your hardware before a large build.

Run tests with `.venv/bin/python -m pytest -q`. See
`.venv/bin/cc-bloom build --help` for all options.
