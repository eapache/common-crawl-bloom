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

# Estimate filter size without downloading or allocating it.
.venv/bin/cc-bloom plan --expected-urls 1000000000 --false-positive-rate 1e-6

# Try one shard from the latest crawl before committing to a full build.
.venv/bin/cc-bloom build --latest 1 --max-shards 1 \
  --expected-urls 20000000 --output sample.bloom

# Combine two snapshots into one filter.
.venv/bin/cc-bloom build \
  --crawl CC-MAIN-2025-33 --crawl CC-MAIN-2025-30 \
  --expected-urls 3000000000 --false-positive-rate 1e-6 \
  --output urls.bloom
```

URL counts above are sizing examples, not measured crawl counts. Size for the
**distinct URLs across all selected crawls**; duplicates do not increase occupancy.
Use `--latest 3` for the newest three snapshots, or repeat `--crawl` for reproducible
selection. Crawl IDs use `CC-MAIN-YYYY-WW`, not calendar month numbers.

Even one shard can contain millions of URLs. `--max-shards` selects the first N
shards and records partial coverage in the artifact; remove it for a complete
build. More snapshots increase historical coverage, build time, and capacity needs.

## Size and memory

Set `--false-positive-rate` (default `1e-6`), or replace it with `--size-mib` for a
fixed bit-array budget. Both require `--expected-urls` to choose the hash count.
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

- Progress is JSON on stderr after each shard; completion is JSON on stdout.
- Downloads retry transient failures. Failed builds do not publish a completed
  filter, and existing output files are never replaced.
- There is no checkpoint/resume yet. Interrupted builds must restart.
- A limited live test succeeded; full-crawl throughput has not been established.
  Benchmark a shard on your hardware before a large build.

Run tests with `.venv/bin/python -m pytest -q`. See
`.venv/bin/cc-bloom build --help` for all options.
