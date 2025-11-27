from pyspark import SparkContext, SparkConf
import random
import time

conf = SparkConf().setAppName("Day2-DataSkew").setMaster("local[*]")
sc = SparkContext(conf=conf)

print("=" * 70)
print("HANDLING DATA SKEW")
print("=" * 70)

# =====================================================
# PROBLEM: Create Skewed Data
# =====================================================
print("\n[PROBLEM] Creating skewed dataset\n")


def create_skewed_data(n):
    """Create highly skewed data where 80% have the same key"""
    data = []
    for i in range(n):
        if i < n * 0.8:
            key = 'popular'       # 80% of records
        else:
            key = f'rare_{i % 20}'  # 20% spread across 20 other keys
        data.append((key, i))
    return data


skewed_data = sc.parallelize(create_skewed_data(10000), numSlices=4)

# Count by key to show distribution
key_distribution = skewed_data.countByKey()
print("Key distribution:")

sorted_dist = sorted(key_distribution.items(), key=lambda x: x[1], reverse=True)
for key, count in sorted_dist[:5]:
    percentage = (count / 10000) * 100
    print(f" {key:15s}: {count:5d} records ({percentage:.1f}%)")

print(f"\nTotal unique keys: {len(key_distribution)}")
print(" Notice: 'popular' has 80% of all data (SKEWED!)")

# =====================================================
# IMPACT: See the Problem
# =====================================================
print("\n[IMPACT] Performance impact of skew\n")

start = time.time()
result_skewed = skewed_data.reduceByKey(lambda a, b: a + b).collect()
time_skewed = time.time() - start

print(f"Aggregation with skew: {time_skewed:.4f}s")
print("Problem: One partition processes 80% of data!")

# =====================================================
# SOLUTION 1: Salting Technique
# =====================================================
print("\n[SOLUTION 1] Salting to handle skew\n")


def add_salt(pair, num_salts=8):
    """Add random salt to key to distribute load"""
    key, value = pair
    salt = random.randint(0, num_salts - 1)
    return (f"{key}_salt{salt}", value)


def remove_salt(pair):
    """Remove salt from key"""
    key, value = pair
    original_key = '_'.join(key.split('_')[:-1])
    return (original_key, value)


salted = skewed_data.map(lambda p: add_salt(p, num_salts=8))

print("Salted key distribution (sample):")
salted_distribution = salted.countByKey()
sample_keys = list(salted_distribution.keys())[:10]

for key in sample_keys:
    print(f" {key:25s}: {salted_distribution[key]:4d} records")

# Perform aggregation with salting
start = time.time()

salted_result = (
    salted
    .reduceByKey(lambda a, b: a + b)
    .map(remove_salt)
    .reduceByKey(lambda a, b: a + b)
)

time_salted = time.time() - start

print(f"\nSalted aggregation: {time_salted:.4f}s")
print(f" Improvement: {time_skewed / time_salted:.2f}x faster")
print("Why: Work distributed across 8 partitions instead of 1")

# =====================================================
# SOLUTION 2: Separate Hot and Cold Keys
# =====================================================
print("\n[SOLUTION 2] Separate processing for hot keys\n")

hot_threshold = 1000
hot_keys = {k for k, count in key_distribution.items() if count > hot_threshold}

print(f"Hot keys (>{hot_threshold} records): {hot_keys}")

hot_data = skewed_data.filter(lambda x: x[0] in hot_keys)
cold_data = skewed_data.filter(lambda x: x[0] not in hot_keys)

print(f"Hot data records: {hot_data.count()}")
print(f"Cold data records: {cold_data.count()}")

start = time.time()

# Hot keys: use salting
hot_aggregated = (
    hot_data
    .map(lambda p: add_salt(p, num_salts=16))
    .reduceByKey(lambda a, b: a + b)
    .map(remove_salt)
    .reduceByKey(lambda a, b: a + b)
)

# Cold keys: regular aggregation
cold_aggregated = cold_data.reduceByKey(lambda a, b: a + b)

# Union results
final_result = hot_aggregated.union(cold_aggregated)
time_separate = time.time() - start

print(f"\nSeparate processing: {time_separate:.4f}s")
print(f"Final result: {final_result.count()} unique keys")

# =====================================================
# SOLUTION 3: Adaptive Partitioning
# =====================================================
print("\n[SOLUTION 3] Increase partitions for skewed keys\n")

print(f"Original partitions: {skewed_data.getNumPartitions()}")

repartitioned = skewed_data.repartition(16)
print(f"After repartition: {repartitioned.getNumPartitions()}")

start = time.time()
result = repartitioned.reduceByKey(lambda a, b: a + b)
time_repart = time.time() - start

print(f"With more partitions: {time_repart:.4f}s")
print("Effect: More parallel tasks, but still some skew")

# =====================================================
# SOLUTION 4: Two-Phase Aggregation
# =====================================================
print("\n[SOLUTION 4] Two-phase aggregation\n")

# Phase 1: Partial aggregation with salting
phase1 = (
    skewed_data
    .map(lambda p: (f"{p[0]}_salt{random.randint(0, 7)}", p[1]))
    .reduceByKey(lambda a, b: a + b)
)

# Phase 2: Final aggregation
phase2 = (
    phase1
    .map(lambda p: ('_'.join(p[0].split('_')[:-1]), p[1]))
    .reduceByKey(lambda a, b: a + b)
)

result = phase2.collect()

print(f"Two-phase result: {len(result)} keys")
print("Advantage: Combines benefits of salting with clean final output")

# =====================================================
# IDENTIFYING SKEW IN SPARK UI
# =====================================================
print("\n" + "=" * 70)
print("IDENTIFYING SKEW IN SPARK UI")
print("=" * 70)

print("""
Signs of data skew in Spark UI:
1. Task Duration Distribution:
   - Most tasks complete quickly
   - A few tasks take much longer (long tail)

2. Shuffle Read/Write:
   - Uneven distribution across tasks

3. Stage Timeline:
   - Many executors finish early
   - One or two executors keep running (hot partitions)

4. GC Time:
   - Slow tasks show high GC time due to memory pressure

How to inspect:
✓ Open Spark UI at http://localhost:4040
✓ Check Stages tab
✓ Look at task duration histogram
✓ Examine shuffle read size per task
✓ Monitor GC time
""")

# =====================================================
# PRACTICE EXERCISES
# =====================================================
print("\n" + "=" * 70)
print("PRACTICE EXERCISES")
print("=" * 70)

print("""
Complete these exercises:
1. Create your own skewed dataset (90% one key, 10% others)
2. Measure performance without optimization
3. Apply salting with different salt counts (4, 8, 16)
4. Compare performance improvements
5. Document when salting helps vs hurts
""")

# Create skewed RDD: 90% key 'hot', 10% random keys
num_records = 1_000_000
skewed_rdd = sc.parallelize(range(num_records)).map(
    lambda x: ("hot" if random.random() < 0.9 else f"key_{x%100}", 1)
)

start = time.time()
result = skewed_rdd.reduceByKey(lambda a, b: a + b).collect()
duration = time.time() - start
print(f"Time without salting: {duration:.4f}s")

def add_salt(pair, num_salts):
    key, value = pair
    # Add random salt for hot key only
    if key == "hot":
        salt = random.randint(0, num_salts-1)
        return (f"{key}_{salt}", value)
    return (key, value)

def remove_salt(pair):
    key, value = pair
    return (key.split("_")[0], value)

# Test different salt counts
for num_salts in [4, 8, 16]:
    start = time.time()
    salted_result = (
        skewed_rdd
        .map(lambda p: add_salt(p, num_salts))
        .reduceByKey(lambda a, b: a + b)
        .map(remove_salt)
        .reduceByKey(lambda a, b: a + b)
        .collect()
    )
    duration = time.time() - start
    print(f"Time with {num_salts} salts: {duration:.4f}s")


# We notice that if num_salts increases, time decreases because the work for "hot" key is split across multiple partitions


# Salting helps when a few keys dominate and cause skewed partitions, but hurts when the dataset is small or too many salts add unnecessary shuffle and overhead.




sc.stop()

