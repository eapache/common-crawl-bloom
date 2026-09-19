"""Portable Bloom filter; writes only at finalization, never on insertion."""

import hashlib
import json
import math
import os
from pathlib import Path
import struct
import tempfile

from .urls import KEY_POLICY, url_key

MAGIC = b"CCBLOOM1"
HASH = "blake2b-128-double-le-v1"
BLOCK = 1024 * 1024


def sizing(expected_urls: int, false_positive_rate: float = 1e-6,
           size_bytes: int | None = None) -> dict:
    if expected_urls <= 0:
        raise ValueError("expected URLs must be positive")
    if not 0 < false_positive_rate < 1:
        raise ValueError("false-positive rate must be between 0 and 1")
    if size_bytes is None:
        size_bytes = math.ceil(-expected_urls * math.log(false_positive_rate)
                               / math.log(2) ** 2 / 8)
    if size_bytes <= 0:
        raise ValueError("filter size must be positive")
    bits = size_bytes * 8
    hashes = max(1, min(64, round(bits / expected_urls * math.log(2))))
    return {"size_bytes": size_bytes, "bits": bits, "hashes": hashes,
            "expected_urls": expected_urls,
            "design_fpr": (-math.expm1(-hashes * expected_urls / bits)) ** hashes}


class BloomFilter:
    def __init__(self, expected_urls: int, false_positive_rate: float = 1e-6,
                 size_bytes: int | None = None):
        self.parameters = sizing(expected_urls, false_positive_rate, size_bytes)
        self.data = bytearray(self.parameters["size_bytes"])
        self.insertions = 0
        self.bits_set = 0
        self.metadata = {}

    def _positions(self, key: bytes):
        a, b = struct.unpack("<QQ", hashlib.blake2b(key, digest_size=16).digest())
        # An odd step avoids short cycles for power-of-two filter sizes.
        b |= 1
        for i in range(self.parameters["hashes"]):
            yield (a + i * b) % self.parameters["bits"]

    def add(self, url: str) -> bool:
        key = url_key(url)
        if key is None:
            return False
        for pos in self._positions(key):
            byte, bit = divmod(pos, 8)
            mask = 1 << bit
            if not self.data[byte] & mask:
                self.data[byte] |= mask
                self.bits_set += 1
        self.insertions += 1
        return True

    def __contains__(self, url: str) -> bool:
        key = url_key(url)
        return key is not None and all(
            self.data[pos // 8] & (1 << (pos % 8)) for pos in self._positions(key)
        )

    def stats(self) -> dict:
        fill = self.bits_set / self.parameters["bits"]
        return {**self.parameters, "insertions": self.insertions,
                "bits_set": self.bits_set, "fill_ratio": fill,
                "estimated_fpr": fill ** self.parameters["hashes"]}

    def save(self, path: str | Path, metadata: dict | None = None):
        """Publish a checksummed artifact atomically, refusing to replace files."""
        path = Path(path)
        header = json.dumps({"version": 1, "hash": HASH, "key_policy": KEY_POLICY,
                             "stats": self.stats(),
                             "source": self.metadata if metadata is None else metadata},
                            sort_keys=True, allow_nan=False).encode()
        if len(header) > 1024 * 1024:
            raise ValueError("filter metadata exceeds header limit")
        prefix = MAGIC + struct.pack("<I", len(header)) + header
        digest = hashlib.sha256(prefix)
        fd, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        try:
            with os.fdopen(fd, "wb") as out:
                out.write(prefix)
                view = memoryview(self.data)
                for offset in range(0, len(view), BLOCK):
                    block = view[offset:offset + BLOCK]
                    out.write(block)
                    digest.update(block)
                out.write(digest.digest())
                out.flush()
                os.fsync(out.fileno())
            # link() is an atomic no-clobber publish on the same filesystem.
            os.link(name, path)
        finally:
            os.unlink(name)

    @classmethod
    def load(cls, path: str | Path):
        with open(path, "rb") as src:
            prefix = src.read(12)
            if len(prefix) != 12 or prefix[:8] != MAGIC:
                raise ValueError("not a supported Bloom filter")
            length, = struct.unpack("<I", prefix[8:])
            if not 0 < length <= 1024 * 1024:
                raise ValueError("invalid Bloom header length")
            raw = src.read(length)
            header = json.loads(raw)
            if (header.get("version") != 1 or header.get("hash") != HASH
                    or header.get("key_policy") != KEY_POLICY):
                raise ValueError("unsupported filter format or URL policy")
            stats = header["stats"]
            size = stats["size_bytes"]
            if (type(size) is not int or size <= 0 or stats["bits"] != size * 8
                    or type(stats["hashes"]) is not int or not 1 <= stats["hashes"] <= 64
                    or not 0 <= stats["bits_set"] <= size * 8
                    or os.fstat(src.fileno()).st_size != 12 + length + size + 32):
                raise ValueError("invalid filter dimensions or file length")
            result = cls.__new__(cls)
            result.parameters = {k: stats[k] for k in
                                 ("size_bytes", "bits", "hashes", "expected_urls", "design_fpr")}
            result.insertions = stats["insertions"]
            result.bits_set = stats["bits_set"]
            result.metadata = header["source"]
            result.data = bytearray(size)
            digest = hashlib.sha256(prefix + raw)
            view = memoryview(result.data)
            for offset in range(0, size, BLOCK):
                block = view[offset:offset + BLOCK]
                if src.readinto(block) != len(block):
                    raise ValueError("truncated filter")
                digest.update(block)
            if src.read(32) != digest.digest():
                raise ValueError("Bloom filter checksum mismatch")
            return result
