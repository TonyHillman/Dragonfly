"""Worker (ZeroMQ REQ client): ask for a task, compute, submit, repeat. Has fault-injection flags."""
import argparse, os, random, sys, time
import zmq
from common import count_primes

ap = argparse.ArgumentParser()
ap.add_argument("--addr", default="tcp://127.0.0.1:5555", help="coordinator address")
ap.add_argument("--delay", type=float, default=0, help="extra sleep per task (slow work)")
ap.add_argument("--crash-prob", type=float, default=0, help="chance per task to die before submitting")
ap.add_argument("--slow-prob", type=float, default=0, help="chance per task to stall for --slow-time")
ap.add_argument("--slow-time", type=float, default=6, help="stall length (set longer than the lease)")
args = ap.parse_args()
me = os.getpid()

sock = zmq.Context().socket(zmq.REQ)
sock.setsockopt(zmq.RCVTIMEO, 5000)   # give up if coordinator is silent for 5s
sock.setsockopt(zmq.LINGER, 0)
sock.connect(args.addr)


def call(msg):
    sock.send_json(msg)
    return sock.recv_json()


while True:
    try:
        job = call({"type": "task"})
    except zmq.Again:
        sys.exit(0)  # coordinator gone
    if job["status"] == "done":
        break
    if job["status"] == "wait":
        time.sleep(0.5)
        continue
    count = count_primes(job["lo"], job["hi"])
    time.sleep(args.delay)
    if random.random() < args.slow_prob:
        print(f"[worker {me}] stalling on task {job['task_id']}", flush=True)
        time.sleep(args.slow_time)
    if random.random() < args.crash_prob:
        print(f"[worker {me}] CRASH holding task {job['task_id']}", flush=True)
        os._exit(1)
    try:
        r = call({"type": "submit", "task_id": job["task_id"],
                  "lease_id": job["lease_id"], "count": count})
    except zmq.Again:
        sys.exit(0)
    if r["result"] != "ok":
        print(f"[worker {me}] task {job['task_id']} submit {r['result']}", flush=True)
