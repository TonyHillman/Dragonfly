"""Coordinator (ZeroMQ transport) with leases, a reaper thread, and submit validation.

Transport: ROUTER socket (workers connect here) <-> proxy <-> DEALER socket (inproc)
           <-> N handler threads, each with a REP socket. Handler threads run
           concurrently, so the shared task table needs protecting.

Modes:
  default  : NAIVE. No lock, submissions accepted blindly. Shows the bugs.
  --lock   : SAFE.  One lock around the task table, plus lease validation
             (stale lease_id rejected, duplicate results ignored).

In both modes a reaper thread requeues tasks whose lease has expired.
"""
import argparse, contextlib, json, os, threading, time, uuid
import zmq
from common import make_chunks

ap = argparse.ArgumentParser()
ap.add_argument("--host", default="127.0.0.1", help="use 0.0.0.0 to accept workers from other machines")
ap.add_argument("--port", type=int, default=5555)
ap.add_argument("--threads", type=int, default=8, help="concurrent request-handler threads")
ap.add_argument("--limit", type=int, default=20_000_000)
ap.add_argument("--chunk", type=int, default=200_000)
ap.add_argument("--lock", action="store_true", help="safe mode: lock + lease validation")
ap.add_argument("--lease", type=float, default=5.0, help="seconds a worker has to finish a task")
ap.add_argument("--race-window", type=float, default=0.005,
                help="sleep inside the critical section to make the race easy to hit")
args = ap.parse_args()

tasks = make_chunks(args.limit, args.chunk)
total = 0
stats = {"double_assigned": 0, "requeued": 0, "stale_rejected": 0, "duplicates_ignored": 0}
lock = threading.Lock()
guard = (lambda: lock) if args.lock else contextlib.nullcontext


def assign():
    """Find a PENDING task, mark it LEASED, issue a lease. Critical section."""
    for t in tasks.values():
        if t["status"] == "PENDING":
            time.sleep(args.race_window)        # widens the read-then-write gap
            if t["status"] != "PENDING":        # someone else grabbed it meanwhile
                stats["double_assigned"] += 1
            t["status"] = "LEASED"
            t["assigned"] += 1
            t["lease_id"] = uuid.uuid4().hex[:8]
            t["lease_expiry"] = time.time() + args.lease
            return {"status": "task", "task_id": t["task_id"], "lo": t["lo"],
                    "hi": t["hi"], "lease_id": t["lease_id"]}
    if any(t["status"] == "LEASED" for t in tasks.values()):
        return {"status": "wait"}
    return {"status": "done"}


def submit(task_id, lease_id, count):
    global total
    t = tasks[task_id]
    if args.lock:
        if t["status"] == "DONE":                # someone already finished it
            stats["duplicates_ignored"] += 1
            return "duplicate"
        if lease_id != t["lease_id"]:            # fencing: lease expired / reassigned
            stats["stale_rejected"] += 1
            return "stale"
    t["status"] = "DONE"
    t["result"] = count
    total += count        # naive mode: every submission is added, even duplicates
    return "ok"


def reap():
    """Requeue tasks whose lease expired (worker presumed dead or too slow)."""
    while True:
        time.sleep(0.5)
        with guard():
            now = time.time()
            for t in tasks.values():
                if t["status"] == "LEASED" and now > t["lease_expiry"]:
                    t["status"] = "PENDING"
                    t["lease_id"] = None         # old holder's lease is now invalid
                    stats["requeued"] += 1
                    print(f"[reaper] lease expired, requeued task {t['task_id']}", flush=True)


def status():
    done = sum(t["status"] == "DONE" for t in tasks.values())
    return {"tasks": len(tasks), "done": done, "total_primes": total, **stats,
            "finished": done == len(tasks)}


def handler(ctx):
    """One request-handling thread: REP socket fed by the proxy."""
    sock = ctx.socket(zmq.REP)
    sock.connect("inproc://backend")
    while True:
        msg = sock.recv_json()
        if msg["type"] == "task":
            with guard():
                sock.send_json(assign())
        elif msg["type"] == "submit":
            with guard():
                result = submit(msg["task_id"], msg.get("lease_id"), msg["count"])
            sock.send_json({"result": result})
        else:
            sock.send_json(status())


def watch_for_finish():
    while not status()["finished"]:
        time.sleep(0.2)
    time.sleep(1)  # let workers see "done"
    print(json.dumps(status(), indent=2), flush=True)
    os._exit(0)


ctx = zmq.Context()
frontend = ctx.socket(zmq.ROUTER)
frontend.bind(f"tcp://{args.host}:{args.port}")
backend = ctx.socket(zmq.DEALER)
backend.bind("inproc://backend")
for _ in range(args.threads):
    threading.Thread(target=handler, args=(ctx,), daemon=True).start()
threading.Thread(target=reap, daemon=True).start()
threading.Thread(target=watch_for_finish, daemon=True).start()
print(f"coordinator (ZeroMQ) on tcp://{args.host}:{args.port}  tasks={len(tasks)}  "
      f"mode={'SAFE' if args.lock else 'NAIVE'}  lease={args.lease}s  threads={args.threads}", flush=True)
zmq.proxy(frontend, backend)
