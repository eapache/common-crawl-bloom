import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

from .bloom import BloomFilter, sizing
from .source import Downloader, collections, shard_urls


def positive_int(value):
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def add_size_options(parser):
    parser.add_argument("--expected-urls", type=positive_int, required=True,
                        help="estimated distinct eligible URLs across all selected crawls")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--false-positive-rate", type=float, default=1e-6)
    group.add_argument("--size-mib", type=positive_int, help="fixed filter allocation in MiB")


def parser():
    root = argparse.ArgumentParser(description="Build exact-URL Bloom filters from Common Crawl")
    commands = root.add_subparsers(dest="command", required=True)
    commands.add_parser("crawls", help="list available crawl IDs")
    plan = commands.add_parser("plan", help="calculate filter RAM and precision without downloading")
    add_size_options(plan)
    build = commands.add_parser("build", help="download and process a union of crawl snapshots")
    selection = build.add_mutually_exclusive_group(required=True)
    selection.add_argument("--crawl", action="append", help="CC-MAIN-YYYY-WW; repeat for a union")
    selection.add_argument("--latest", type=positive_int, help="use the latest N crawl snapshots")
    add_size_options(build)
    build.add_argument("--output", type=Path, required=True)
    build.add_argument("--max-range-mib", type=positive_int, default=64,
                       help="maximum single compressed-column/footer read in MiB")
    build.add_argument("--batch-size", type=positive_int, default=65536)
    build.add_argument("--max-shards", type=positive_int,
                       help="process only the first N shards total (partial coverage)")
    build.add_argument("--timeout", type=float, default=60)
    build.add_argument("--retries", type=int, default=3)
    build.add_argument("--allow-overfilled", action="store_true",
                       help="publish even if occupancy-estimated FPR exceeds design by >10%%")
    inspect = commands.add_parser("inspect", help="verify checksum and print artifact metadata")
    inspect.add_argument("filter", type=Path)
    check = commands.add_parser("check", help="test URLs; positive means possibly present")
    check.add_argument("filter", type=Path)
    check.add_argument("urls", nargs="*", help="URLs, or one per line on stdin")
    return root


def run(args):
    if args.command == "crawls":
        for crawl in collections(Downloader()):
            print(f"{crawl['id']}\t{crawl.get('name', '')}")
        return 0
    if args.command in ("inspect", "check"):
        bloom = BloomFilter.load(args.filter)
        if args.command == "inspect":
            print(json.dumps({"stats": bloom.stats(), "source": bloom.metadata}, indent=2))
            return 0
        urls = args.urls or (line.rstrip("\r\n") for line in sys.stdin)
        all_present = True
        for url in urls:
            present = url in bloom
            print(json.dumps({"url": url, "possibly_present": present}))
            all_present &= present
        return 0 if all_present else 1
    size_bytes = args.size_mib * 1024 * 1024 if args.size_mib else None
    dimensions = sizing(args.expected_urls, args.false_positive_rate, size_bytes)
    if args.command == "plan":
        print(json.dumps(dimensions, indent=2))
        return 0
    if args.output.exists():
        raise ValueError(f"output already exists: {args.output}")
    if not args.output.parent.is_dir():
        raise ValueError(f"output directory does not exist: {args.output.parent}")
    downloader = Downloader(args.timeout, args.retries)
    if args.latest:
        available = collections(downloader)
        if len(available) < args.latest:
            raise ValueError("fewer crawls available than requested")
        crawls = [item["id"] for item in available[:args.latest]]
    else:
        crawls = list(dict.fromkeys(args.crawl))
    urls = [url for crawl in crawls for url in shard_urls(crawl, downloader)]
    total_shards = len(urls)
    if args.max_shards:
        urls = urls[:args.max_shards]
    print(json.dumps({"crawls": crawls, "selected_shards": len(urls),
                      "available_shards": total_shards, "filter": dimensions,
                      "max_range_bytes": args.max_range_mib * 1024 * 1024}),
          file=sys.stderr, flush=True)
    bloom = BloomFilter(args.expected_urls, args.false_positive_rate, size_bytes)
    from .build import build
    stats = build(urls, bloom, downloader, args.max_range_mib * 1024 * 1024,
                  args.batch_size,
                  progress=lambda record: print(json.dumps(record), file=sys.stderr, flush=True))
    if bloom.stats()["estimated_fpr"] > dimensions["design_fpr"] * 1.1:
        if not args.allow_overfilled:
            raise ValueError("filter overfilled: estimated FPR exceeds design by >10%; "
                             "increase --expected-urls or --size-mib, or use --allow-overfilled")
        print("WARNING: publishing an overfilled filter; inspect estimated_fpr", file=sys.stderr)
    metadata = {"created_at": datetime.now(timezone.utc).isoformat(),
                "crawls": crawls, "processed_shards": len(urls),
                "shard_list_sha256": hashlib.sha256("\n".join(urls).encode()).hexdigest(),
                "available_shards": total_shards,
                "partial": len(urls) < total_shards, "status_policy": "200 <= fetch_status < 300",
                "build": stats}
    bloom.save(args.output, metadata)
    print(json.dumps({"output": str(args.output), **bloom.stats()}))
    return 0


def main():
    args = parser().parse_args()
    try:
        return run(args)
    except KeyboardInterrupt:
        print("interrupted; no completed filter published", file=sys.stderr)
        return 130
    except (OSError, ValueError, RuntimeError, KeyError, TypeError, MemoryError, EOFError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
