import math
import struct

import pytest

from common_crawl_bloom import BloomFilter
from common_crawl_bloom.bloom import sizing
from common_crawl_bloom.urls import url_key


def test_sizing():
    dimensions = sizing(1_000_000, 1e-6)
    assert dimensions["size_bytes"] == 3_594_397
    assert dimensions["hashes"] == 20
    assert math.isclose(dimensions["design_fpr"], 1e-6, rel_tol=.01)
    assert sizing(1000, size_bytes=2048)["size_bytes"] == 2048


@pytest.mark.parametrize("args", [(0,), (-1,), (1, 0), (1, 1), (1, float("nan")),
                                   (1, .1, 0), (1, .1, -1)])
def test_bad_sizing(args):
    with pytest.raises(ValueError):
        sizing(*args)


def test_membership_roundtrip_and_duplicates(tmp_path):
    bloom = BloomFilter(1000, 1e-6)
    urls = [f"https://example.org/path?id={i}" for i in range(1000)]
    for url in urls:
        assert bloom.add(url)
    count = bloom.bits_set
    for url in urls:
        bloom.add(url)
    assert bloom.bits_set == count
    assert all(url in bloom for url in urls)
    assert "https://example.org/path?id=secret" not in bloom
    path = tmp_path / "filter.bloom"
    bloom.save(path, {"crawls": ["CC-MAIN-2025-33"]})
    loaded = BloomFilter.load(path)
    assert loaded.stats() == bloom.stats()
    assert loaded.metadata["crawls"] == ["CC-MAIN-2025-33"]
    assert all(url in loaded for url in urls)
    with pytest.raises(FileExistsError):
        bloom.save(path)
    assert not list(tmp_path.glob(".filter.bloom.*"))


def test_empirical_false_positives():
    bloom = BloomFilter(10000, .01)
    for i in range(10000):
        bloom.add(f"https://example.org/{i}")
    observed = sum(f"https://example.org/{i}" in bloom for i in range(10000, 30000)) / 20000
    assert .006 < observed < .016
    assert abs(observed - bloom.stats()["estimated_fpr"]) < .005


@pytest.mark.parametrize("change", ["bit", "truncate", "append", "magic", "length", "header"])
def test_corruption_rejected(tmp_path, change):
    path = tmp_path / "filter.bloom"
    bloom = BloomFilter(100)
    bloom.add("https://example.org/")
    bloom.save(path)
    data = bytearray(path.read_bytes())
    if change == "bit":
        data[-33] ^= 1
    elif change == "truncate":
        del data[-1:]
    elif change == "append":
        data += b"x"
    elif change == "magic":
        data[0] ^= 1
    elif change == "header":
        data = data.replace(b'"version": 1', b'"version": 2')
    else:
        data[8:12] = struct.pack("<I", 2**32 - 1)
    path.write_bytes(data)
    with pytest.raises(ValueError):
        BloomFilter.load(path)


@pytest.mark.parametrize("url", ["ftp://example.org/", "https://user:pw@example.org/",
    "https://example.org/#fragment", "https://example.org/a b", "https://example.org/\n",
    "https://example.org/\\a", "https://example.org:99999/", "https://example.org:0/",
    "https://[broken/", "https:///a", "https://example.org/%zz", "https://é.org/", None])
def test_ineligible_urls(url):
    bloom = BloomFilter(10)
    assert url_key(url) is None
    assert not bloom.add(url)
    assert url not in bloom


def test_keys_preserve_every_component():
    original = "https://example.org/path?a=1&b=%2F"
    assert url_key(original) == original.encode()
    variants = [original.replace("https", "http"), original.replace("path", "Path"),
                original + "&secret=abc", original.replace("a=1&b=%2F", "b=%2F&a=1"),
                original.replace("%2F", "%2f"), original.replace(".org", ".org:443")]
    assert all(url_key(v) != url_key(original) for v in variants)
