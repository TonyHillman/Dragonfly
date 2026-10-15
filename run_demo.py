"""Cross-platform demo launcher (Windows/macOS/Linux).

usage: python run_demo.py [--lock] [--workers N] [--limit L] [--lease S]
                          [--delay S] [--crash-prob P] [--slow-prob P]
                          [--kill-after S] [--respawn]
"""
import argparse, subprocess, sys, time

ap = argparse.ArgumentParser()
ap.add_argument("--lock", action="store_true", help="safe mode")
ap.add_argument("--workers", type=int, default=4)
ap.add_argument("--limit", type=int, default=20_000_000)
ap.add_argument("--lease", type=float, default=5.0)
ap.add_argument("--delay", type=float, default=0.0, help="per-task worker delay")
ap.add_argument("--crash-prob", type=float, default=0.0)
ap.add_argument("--slow-prob", type=float, default=0.0)
ap.add_argument("--kill-after", type=float, default=0.0, help="kill worker #0 after this many seconds")
ap.add_argument("--respawn", action="store_true", help="restart workers that crash")
a = ap.parse_args()

cmd = [sys.executable, "coordinator.py", "--limit", str(a.limit), "--lease", str(a.lease)]
if a.lock:
    cmd.append("--lock")
coord = subprocess.Popen(cmd)
time.sleep(0.5)


def spawn():
    return subprocess.Popen([sys.executable, "worker.py", "--delay", str(a.delay),
                             "--crash-prob", str(a.crash_prob), "--slow-prob", str(a.slow_prob),
                             "--slow-time", str(a.lease * 2)])


workers = [spawn() for _ in range(a.workers)]
start, killed = time.time(), False
try:
    while coord.poll() is None:
        time.sleep(0.2)
        if a.kill_after and not killed and time.time() - start >= a.kill_after:
            print(f"[demo] killing worker pid {workers[0].pid}", flush=True)
            workers[0].kill()
            killed = True
        if a.respawn:
            for i, w in enumerate(workers):
                if w.poll() not in (None, 0):
                    print(f"[demo] respawning worker {i}", flush=True)
                    workers[i] = spawn()
finally:
    for w in workers:
        w.terminate()
