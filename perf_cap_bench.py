import time
import numpy as np
import kvstore_pb2
from client import ReplicatedKVClient

def run_benchmark():
    addrs = ["localhost:50051", "localhost:50052", "localhost:50053"]
    client = ReplicatedKVClient(replica_addresses=addrs, leaseholder_addr="localhost:50051")
    
    # Инициализация ключа
    client.put("bench_key", "bench_val", strategy=kvstore_pb2.QUORUM_CP)
    
    iterations = 1000
    
    # Scenario 1: LINEARIZABLE
    latencies_lin = []
    successes_lin = 0
    for _ in range(iterations):
        start = time.perf_counter()
        found, val, err = client.get("bench_key", consistency=kvstore_pb2.LINEARIZABLE)
        dur = (time.perf_counter() - start) * 1000.0
        if found:
            successes_lin += 1
            latencies_lin.append(dur)
            
    # Scenario 2: FOLLOWER_STALE
    latencies_stale = []
    successes_stale = 0
    for _ in range(iterations):
        start = time.perf_counter()
        found, val, err = client.get("bench_key", consistency=kvstore_pb2.FOLLOWER_STALE, target_addr="localhost:50052")
        dur = (time.perf_counter() - start) * 1000.0
        if found:
            successes_stale += 1
            latencies_stale.append(dur)

    print("--- PACELC Benchmark Results ---")
    print(f"Linearizable (Leaseholder Remote):")
    print(f"  Median Latency: {np.median(latencies_lin):.3f} ms")
    print(f"  p95 Latency:    {np.percentile(latencies_lin, 95):.3f} ms")
    print(f"  Availability:   {(successes_lin/iterations)*100:.1f}%\n")

    print(f"Stale (Local Follower):")
    print(f"  Median Latency: {np.median(latencies_stale):.3f} ms")
    print(f"  p95 Latency:    {np.percentile(latencies_stale, 95):.3f} ms")
    print(f"  Availability:   {(successes_stale/iterations)*100:.1f}%")

if __name__ == "__main__":
    run_benchmark()