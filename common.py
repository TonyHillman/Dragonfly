"""Shared helpers: prime counting and chunking."""
from math import isqrt


def count_primes(lo, hi):
    """Count primes in [lo, hi) using a segmented sieve."""
    lo = max(lo, 2)
    if hi <= lo:
        return 0
    r = isqrt(hi - 1)
    small = bytearray([1]) * (r + 1)
    small[:2] = b"\x00\x00"[: min(2, r + 1)]
    for i in range(2, isqrt(r) + 1):
        if small[i]:
            small[i * i :: i] = bytes(len(range(i * i, r + 1, i)))
    seg = bytearray([1]) * (hi - lo)
    for p in range(2, r + 1):
        if small[p]:
            start = max(p * p, ((lo + p - 1) // p) * p)
            seg[start - lo :: p] = bytes(len(range(start - lo, hi - lo, p)))
    return sum(seg)


def make_chunks(limit, chunk_size):
    """Split [2, limit) into tasks of roughly chunk_size numbers each."""
    tasks, tid, lo = {}, 0, 2
    while lo < limit:
        hi = min(lo + chunk_size, limit)
        tasks[tid] = {"task_id": tid, "lo": lo, "hi": hi,
                      "status": "PENDING", "assigned": 0, "result": None,
                      "lease_id": None, "lease_expiry": 0.0}
        tid += 1
        lo = hi
    return tasks
