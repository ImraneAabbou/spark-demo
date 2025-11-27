from pyspark import SparkContext, SparkConf, StorageLevel
import time

conf = SparkConf().setAppName("Day2-Persistence").setMaster("local[*]")
sc = SparkContext(conf=conf)

print("=" * 70)
print("RDD PERSISTENCE STRATEGIES")
print("=" * 70)

# Create large dataset
large_data = sc.parallelize(range(1, 1000001), numSlices=8)

# =====================================================
# EXPERIMENT 1: Without Caching
# =====================================================
print("\n[EXPERIMENT 1] Without caching - Multiple actions\n")

# Transform
transformed = large_data.map(lambda x: x * 2).filter(lambda x: x % 3 == 0)

# Multiple actions - each recomputes from scratch
start = time.time()
count1 = transformed.count()
time1 = time.time() - start
print(f"First count: {count1:7d} | Time: {time1:.4f}s")

start = time.time()
sum1 = transformed.sum()
time2 = time.time() - start
print(f"Sum: {sum1:10d} | Time: {time2:.4f}s")

start = time.time()
max1 = transformed.max()
time3 = time.time() - start
print(f"Max: {max1:10d} | Time: {time3:.4f}s")

total_time_no_cache = time1 + time2 + time3
print(f"\n Total time without cache: {total_time_no_cache:.4f}s")
print(" Note: Each action recomputes the entire pipeline")

# =====================================================
# EXPERIMENT 2: With Caching (MEMORY_ONLY)
# =====================================================
print("\n[EXPERIMENT 2] With cache() - MEMORY_ONLY\n")

# Transform and cache
cached = large_data.map(lambda x: x * 2).filter(lambda x: x % 3 == 0).cache()

# First action materializes cache
start = time.time()
count2 = cached.count()
time1 = time.time() - start
print(f"First count (materialize): {count2:7d} | Time: {time1:.4f}s")

# Subsequent actions use cache
start = time.time()
sum2 = cached.sum()
time2 = time.time() - start
print(f"Sum (from cache): {sum2:10d} | Time: {time2:.4f}s ⚡")

start = time.time()
max2 = cached.max()
time3 = time.time() - start
print(f"Max (from cache): {max2:10d} | Time: {time3:.4f}s ⚡")

total_time_with_cache = time1 + time2 + time3
print(f"\n Total time with cache: {total_time_with_cache:.4f}s")
print(f" Speedup: {total_time_no_cache/total_time_with_cache:.2f}x faster")

# Unpersist to free memory
cached.unpersist()

# =====================================================
# EXPERIMENT 3: Different Storage Levels
# =====================================================
print("\n[EXPERIMENT 3] Comparing storage levels\n")

test_rdd = large_data.map(lambda x: (x, x * 2, x * 3))

storage_levels = [
    (StorageLevel.MEMORY_ONLY, "MEMORY_ONLY"),
    (StorageLevel.MEMORY_AND_DISK, "MEMORY_AND_DISK"),
    # next line was commented as it produces an exception that the specified storage level doesn't exist
    # (StorageLevel.MEMORY_ONLY_SER, "MEMORY_ONLY_SER"),
    (StorageLevel.DISK_ONLY, "DISK_ONLY")
]

print("Storage Level Comparison:")
print(f"{'Level':<25s} {'Materialize':<15s} {'Read':<15s}")
print("-" * 55)

for level, name in storage_levels:
    persisted = test_rdd.persist(level)

    start = time.time()
    persisted.count()
    materialize_time = time.time() - start

    start = time.time()
    persisted.take(100)
    read_time = time.time() - start

    print(f"{name:<25s} {materialize_time:<15.4f} {read_time:<15.4f}")

    persisted.unpersist()

# =====================================================
# EXPERIMENT 4: When Caching Helps vs Hurts
# =====================================================
print("\n[EXPERIMENT 4] When caching helps vs hurts\n")

print("Scenario 1: Multiple uses (caching HELPS)")
rdd1 = sc.parallelize(range(1, 100001)).cache()
t1 = time.time()
rdd1.count()
rdd1.sum()
rdd1.max()
time_cached = time.time() - t1
print(f" Time with cache: {time_cached:.4f}s")
rdd1.unpersist()

print("\nScenario 2: Single use (caching WASTES resources)")
rdd2 = sc.parallelize(range(1, 100001)).cache()
t2 = time.time()
rdd2.count()
time_single = time.time() - t2
print(f" Time with cache (but used once): {time_single:.4f}s")
print(" Cache overhead wasted!")
rdd2.unpersist()

print("\nScenario 3: Cheap computation (caching NOT worth it)")
cheap_rdd = sc.parallelize(range(1, 1001))
cheap_cached = cheap_rdd.map(lambda x: x + 1).cache()

t3 = time.time()
cheap_cached.count()
cheap_cached.sum()
time_cheap = time.time() - t3
print(f" Time with cache: {time_cheap:.4f}s")
print(" Recomputing would be faster than caching overhead")
cheap_cached.unpersist()

print("\nScenario 4: Expensive computation, multiple uses (caching HELPS)")
expensive_rdd = sc.parallelize(range(1, 10001))
expensive_cached = expensive_rdd.map(lambda x: sum(range(x))).cache()

t4 = time.time()
expensive_cached.count()
expensive_cached.sum()
expensive_cached.max()
time_expensive = time.time() - t4
print(f" Time with cache: {time_expensive:.4f}s")
print(" Cache saves expensive recomputation!")
expensive_cached.unpersist()

# =====================================================
# EXPERIMENT 5: Checkpoint for Long Lineage
# =====================================================
print("\n[EXPERIMENT 5] Checkpoint for long lineage\n")

sc.setCheckpointDir("/tmp/spark-checkpoint")

long_lineage = sc.parallelize(range(1, 10001))
for i in range(10):
    long_lineage = long_lineage.map(lambda x: x + 1)

print("Lineage length before checkpoint: ~10 transformations")
print("Risk: Long recovery time on failure")

long_lineage.checkpoint()
long_lineage.count()
print("\n After checkpoint:")
print(" - Lineage is truncated")
print(" - Data saved to reliable storage")
print(" - Faster recovery on failure")

# =====================================================
# STORAGE LEVEL GUIDE
# =====================================================
print("\n" + "=" * 70)
print("STORAGE LEVEL DECISION GUIDE")
print("=" * 70)

print("""
MEMORY_ONLY (default):
✓ Use when: Data fits in memory, fastest access needed
✓ Pros: Fastest, no serialization overhead
✗ Cons: May fail with OOM, data loss if evicted

MEMORY_AND_DISK:
✓ Use when: Safe fallback needed, data might not fit
✓ Pros: Won't fail with OOM, spillover to disk
✗ Cons: Slower disk access for spilled partitions

MEMORY_ONLY_SER:
✓ Use when: Memory constrained, willing to trade CPU for space
✓ Pros: Smaller memory footprint
✗ Cons: Serialization/deserialization overhead

MEMORY_AND_DISK_SER:
✓ Use when: Large dataset, memory limited, safe fallback
✓ Pros: Space efficient with disk spillover
✗ Cons: Serialization + disk I/O overhead

DISK_ONLY:
✓ Use when: Memory very limited, cost-effective storage
✓ Pros: Always works, predictable
✗ Cons: Slowest option

OFF_HEAP:
✓ Use when: Large memory available, avoid GC
✓ Pros: No GC overhead, precise memory control
✗ Cons: Complex configuration, external memory needed
""")

# =====================================================
# BEST PRACTICES
# =====================================================
print("\n" + "=" * 70)
print("CACHING BEST PRACTICES")
print("=" * 70)

print("""
When to cache:
- RDD used multiple times in same job
- Iterative algorithms (ML training)
- Interactive queries on same dataset
- Expensive transformations worth saving
- Intermediate results in complex pipelines

When NOT to cache:
- RDD used only once
- Large RDDs that don't fit in memory
- Cheap transformations
- Low memory environment
- Data that changes frequently
""")

# =====================================================
# PRACTICE EXERCISES
# =====================================================
print("\n" + "=" * 70)
print("PRACTICE EXERCISES")
print("=" * 70)

print("""
Complete these exercises:
1. Create an iterative algorithm and measure cache benefit
2. Test different storage levels with your data
3. Implement checkpoint for a 20-step transformation
4. Monitor Spark UI Storage tab during caching
5. Compare memory usage of serialized vs non-serialized
""")



sc.stop()

