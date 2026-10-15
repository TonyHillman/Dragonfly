# Dragonfly: A Fault-Tolerant Distributed Task Queue

Dragonfly is a small distributed batch-processing system written in Python using [ZeroMQ](https://zeromq.org/). A **coordinator** splits a job into tasks and hands them to any number of **worker** processes. It guarantees that every task's result is counted **exactly once**, even when workers crash, stall, or report back late.

Counting primes below a limit is the stand-in workload: it is CPU-heavy, easy to split into ranges, and easy to verify against a single-process run. Swapping in a different job means replacing one function (`count_primes` in `common.py`).

The project demonstrates three things:

1. **A real race condition** (double task assignment), shown in a deliberately naive mode and fixed with a lock.
2. **Failure recovery** using leases and a reaper thread, so work held by a dead worker is reassigned.
3. **Safe handling of late and duplicate results** using lease IDs (fencing tokens).

## Contents

- [How it works](#how-it-works)
- [Requirements and installation](#requirements-and-installation)
- [Quick start](#quick-start)
- [Demo scenarios](#demo-scenarios)
- [Running coordinator and workers manually](#running-coordinator-and-workers-manually)
- [Running on multiple machines](#running-on-multiple-machines)
- [Benchmark](#benchmark)
- [Command-line options](#command-line-options)
- [Project files](#project-files)
- [Limitations](#limitations)

## How it works

```
 Worker 1 (REQ) ----\
 Worker 2 (REQ) -----+--> tcp://host:5555 --> ROUTER --> proxy --> DEALER --> 8 handler threads (REP)
 Worker N (REQ) ----/                                                         + reaper thread
                                                                              (the coordinator)
```

**Coordinator** (one process) owns all state: a task table, the leases, and the running total. It never counts primes itself.

**Workers** (any number of processes) loop: ask for a task, count primes in that range, submit the result.

**Messages** are JSON over ZeroMQ, distinguished by a `type` field:

| Message | Sent by | Meaning |
|---|---|---|
| `task` | Worker | "Give me work." Reply is a task with a lease, `wait`, or `done`. |
| `submit` | Worker | "Here is my result for task *t* under lease *l*." |
| `status` | Anyone | "Report progress and statistics." |

**Task lifecycle.** Each task is `PENDING`, `LEASED`, or `DONE`. When a worker asks for work, the coordinator marks a pending task `LEASED`, issues a random `lease_id`, and sets a deadline (5 seconds by default). A **reaper thread** checks every 0.5 seconds and returns any task whose lease has expired to `PENDING`, clearing its `lease_id`.

**Submit validation.** In safe mode a result is accepted only if the task is not already `DONE` and the submitted `lease_id` matches the task's current lease. Otherwise the submission is ignored as a `duplicate` or rejected as `stale`.

**Why ZeroMQ is set up this way.** A single `REP` socket would handle one request at a time and silently hide every race condition. Instead the coordinator uses ZeroMQ's multithreaded-server pattern (`ROUTER` to `DEALER` to several `REP` handler threads), so requests are processed concurrently, as in a real server.

### The two modes

| Mode | Flag | Behavior |
|---|---|---|
| **Naive** | *(default)* | No lock, and every submission is added to the total. Exhibits the bugs. |
| **Safe** | `--lock` | One lock around the shared state, plus lease validation. Correct. |

In naive mode the coordinator sleeps 5 ms between checking a task's status and marking it leased. That artificially widens the race window so the bug is easy to reproduce. Real races have much narrower windows, which is why they are hard to catch. Use `--race-window 0` to remove it.

## Requirements and installation

- Python 3.8 or newer
- [pyzmq](https://pypi.org/project/pyzmq/)

```
git clone https://github.com/TonyHillman/Dragonfly
cd Dragonfly
pip install -r requirements.txt
```

If `python` is not recognized on Windows, use `py` instead. If you get `No module named 'zmq'`, run `python -m pip install pyzmq` so it installs into the same Python that runs the scripts.

## Quick start

Run these from the project folder. Everything runs on your machine, and `run_demo.py` starts the coordinator and workers for you.

```
python baseline.py 20000000
python run_demo.py --workers 4
python run_demo.py --lock --workers 4
```

1. `baseline.py` counts primes sequentially in one process. This is the correct answer: **1,270,607** primes below 20,000,000.
2. The second command runs **naive mode**. The final total is wrong and `double_assigned` is above zero.
3. The third command runs **safe mode**. The total is 1,270,607 and `double_assigned` is 0.

Example output from a naive run (yours will differ, since the race depends on thread timing):

```
{
  "tasks": 100,
  "done": 100,
  "total_primes": 4646885,
  "double_assigned": 266,
  "requeued": 0,
  "stale_rejected": 0,
  "duplicates_ignored": 0,
  "finished": true
}
```

## Demo scenarios

All use safe mode and should finish with a total of **1,270,607**.

**Kill a worker mid-run.** The launcher kills one worker after 1.5 seconds. Its task is requeued when the lease expires and another worker finishes it.

```
python run_demo.py --lock --delay 0.2 --lease 2 --kill-after 1.5
```

Look for `[demo] killing worker pid ...` followed by `[reaper] lease expired, requeued task N`.

**Random worker crashes.** Each task has a 10% chance of killing its worker. `--respawn` restarts crashed workers so the pool does not run out.

```
python run_demo.py --lock --delay 0.1 --lease 2 --crash-prob 0.1 --respawn
```

**Workers stalling past their lease.** Each task has a 10% chance of stalling longer than the lease, so the worker comes back late. This produces `stale` and `duplicate` rejections.

```
python run_demo.py --lock --delay 0.1 --lease 2 --slow-prob 0.1
```

Look for `[worker ...] task N submit stale` or `submit duplicate`, and nonzero `stale_rejected` or `duplicates_ignored` in the final output.

## Running coordinator and workers manually

`run_demo.py` is only a launcher. To see the coordinator and workers as separate processes, open several terminals in the project folder.

```
# Terminal 1
python coordinator.py --lock --chunk 1000000 --lease 6

# Terminals 2, 3, 4
python worker.py --delay 2
```

The default job is small enough that one worker finishes it in about a second. `--chunk 1000000` makes 20 tasks instead of 100, and `--delay 2` makes each task take about 2 seconds, so you have time to start more workers. Keep `--lease` longer than `--delay`, otherwise every task expires before it is submitted.

To simulate a failure, press Ctrl+C in a worker's terminal while it is mid-task. After the lease expires the coordinator prints a `[reaper]` line and another worker finishes the task.

## Benchmark

`benchmark.py` runs the same workload with different numbers of workers in safe mode, verifies each result against the sequential answer, and prints a speedup table.

```
python benchmark.py
python benchmark.py --workers 1 2 4 8 12
python benchmark.py --limit 1000000000 --chunk 10000000
```

Sample result (primes below 400,000,000 in 100 tasks, on a machine with 14 logical cores):

| Workers | Time (s) | Speedup vs. 1 worker | Efficiency | Correct |
|---|---|---|---|---|
| 1 | 3.84 | 1.00x | 100% | True |
| 2 | 2.11 | 1.82x | 91% | True |
| 4 | 1.35 | 2.84x | 71% | True |
| 8 | 1.21 | 3.19x | 40% | True |

The sequential single-process run took 3.84 s, so 8 workers are about **3.2x faster than sequential**.

Notes on reading the numbers:

- **Efficiency falls as workers increase** because the run is only about a second long, so fixed startup costs and message round trips are a large share of it (Amdahl's law). Logical cores are also not all equal (performance vs. efficiency cores, hyperthreads), and tasks finish unevenly at the end of the run.
- A larger `--limit` dilutes the fixed overhead and improves efficiency.
- Single runs vary by several percent. Repeat the benchmark and use a typical result.

## Command-line options

### `coordinator.py`

| Option | Default | Description |
|---|---|---|
| `--lock` | off | Safe mode: lock around shared state plus lease validation |
| `--host` | `127.0.0.1` | Address to listen on (`0.0.0.0` to accept other machines) |
| `--port` | `5555` | Port to listen on |
| `--threads` | `8` | Concurrent request-handler threads |
| `--limit` | `20000000` | Count primes below this number |
| `--chunk` | `200000` | Numbers per task (20M / 200k = 100 tasks) |
| `--lease` | `5.0` | Seconds a worker has to finish a task |
| `--race-window` | `0.005` | Artificial delay (seconds) inside the critical section to expose the race |

### `worker.py`

| Option | Default | Description |
|---|---|---|
| `--addr` | `tcp://127.0.0.1:5555` | Coordinator address |
| `--delay` | `0` | Extra sleep per task, to simulate slow work |
| `--crash-prob` | `0` | Chance per task that the worker dies before submitting |
| `--slow-prob` | `0` | Chance per task that the worker stalls for `--slow-time` |
| `--slow-time` | `6` | Stall length in seconds (set longer than the lease) |

### `run_demo.py`

Starts one coordinator and N workers and passes the settings through.

| Option | Default | Description |
|---|---|---|
| `--lock` | off | Safe mode |
| `--workers` | `4` | Number of workers |
| `--limit` | `20000000` | Count primes below this number |
| `--lease` | `5.0` | Lease length in seconds |
| `--delay` | `0` | Per-task worker delay |
| `--crash-prob` | `0` | Per-task crash probability |
| `--slow-prob` | `0` | Per-task stall probability (stall lasts twice the lease) |
| `--kill-after` | `0` | Kill worker #0 after this many seconds (0 = never) |
| `--respawn` | off | Restart workers that exit with an error |

### `benchmark.py`

| Option | Default | Description |
|---|---|---|
| `--limit` | `400000000` | Count primes below this number |
| `--chunk` | `4000000` | Numbers per task |
| `--workers` | `1 2 4 8` | Worker counts to test |

### `baseline.py`

```
python baseline.py [limit]
```

Counts primes below `limit` (default 20,000,000) sequentially.

## Project files

| File | Purpose |
|---|---|
| `common.py` | Segmented-sieve prime counter and task chunking |
| `coordinator.py` | Task table, leases, reaper thread, ZeroMQ server |
| `worker.py` | Worker loop with fault-injection options |
| `run_demo.py` | Cross-platform launcher: starts processes, kills or respawns workers |
| `benchmark.py` | Speedup measurement with correctness checks |
| `baseline.py` | Sequential ground truth |
| `requirements.txt` | Python dependencies (`pyzmq`) |

## Limitations

- **The coordinator is a single point of failure.** Its state is kept in memory, so if it dies the job is lost. Persisting state, or replicating the coordinator with a consensus protocol, would address this.
- **Tasks are assumed to be deterministic and free of side effects**, so re-running a task is harmless. Tasks with side effects would need idempotency keys.
- **Lease lengths are fixed.** Too short causes needless requeues, too long delays recovery. Heartbeats or adaptive leases would help.
- **A single lock** is simple and correct, but would limit scaling to very large numbers of workers.
- **No security.** Messages are unencrypted and unauthenticated, so anyone who can reach the port can act as a worker. Use only on a trusted network.
- **Failure model:** handles crashed, slow, and late workers. Does not handle malicious workers or message corruption beyond what TCP provides.
- **Benchmarks** are single runs on one machine, and the demos run all processes locally unless you follow the multi-machine steps above.
