"""Sequential baseline: the ground truth the distributed answer must match."""
import sys, time
from common import count_primes

limit = int(sys.argv[1]) if len(sys.argv) > 1 else 20_000_000
t0 = time.time()
print(f"primes below {limit:,}: {count_primes(2, limit):,}  ({time.time()-t0:.2f}s)")