"""Speedup benchmark: same workload with 1, 2, 4, 8 workers (safe mode).

usage: python benchmark.py [--limit L] [--chunk C] [--workers 1 2 4 8]
Verifies every run against the sequential answer.
"""
import argparse, json, os, subprocess, sys, time
import zmq
from common import count_primes

ap = argparse.ArgumentParser()
ap.add_argument("--limit", type=int, default=400_000_000)
ap.add_argument("--chunk", type=int, default=4_000_000)
ap.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4, 8])
a = ap.parse_args()

# Sequential reference (same chunking, so memory stays small)
print(f"limit={a.limit:,} chunk={a.chunk:,} cpu_cores={os.cpu_count()}", flush=True)
t0 = time.time()
expected = sum(count_primes(lo, min(lo + a.chunk, a.limit)) for lo in range(2, a.limit, a.chunk))
seq_time = time.time() - t0
print(f"sequential: {expected:,} primes in {seq_time:.2f}s\n", flush=True)


ctx = zmq.Context()


def finished(port):
    s = ctx.socket(zmq.REQ)
    s.setsockopt(zmq.RCVTIMEO, 300)
    s.setsockopt(zmq.LINGER, 0)
    s.connect(f"tcp://127.0.0.1:{port}")
    try:
        s.send_json({"type": "status"})
        return s.recv_json()["finished"]
    except zmq.Again:
        return None  # coordinator not up yet
    finally:
        s.close()


rows = []
for n in a.workers:
    port = 8100 + n
    coord = subprocess.Popen(
        [sys.executable, "coordinator.py", "--port", str(port), "--limit", str(a.limit),
         "--chunk", str(a.chunk), "--lock", "--race-window", "0"],
        stdout=subprocess.PIPE, text=True)
    while finished(port) is None:
        time.sleep(0.05)
    start = time.time()
    workers = [subprocess.Popen([sys.executable, "worker.py", "--addr", f"tcp://127.0.0.1:{port}"])
               for _ in range(n)]
    while not finished(port):
        time.sleep(0.05)
    elapsed = time.time() - start
    out, _ = coord.communicate(timeout=30)
    for w in workers:
        w.wait(timeout=30)
    total = json.loads(out[out.index("{"):])["total_primes"]
    rows.append((n, elapsed, total == expected))
    print(f"  {n} workers: {elapsed:.2f}s  correct={total == expected}", flush=True)

base = rows[0][1]
print(f"\n{'workers':>8} {'time (s)':>10} {'speedup':>9} {'efficiency':>11} {'correct':>8}")
for n, t, ok in rows:
    print(f"{n:>8} {t:>10.2f} {base / t:>8.2f}x {base / t / n:>10.0%} {str(ok):>8}")
print(f"\nsequential (no network, 1 process): {seq_time:.2f}s")
