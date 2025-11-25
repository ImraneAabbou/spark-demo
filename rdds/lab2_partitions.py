from pyspark import SparkContext, SparkConf
# Create Spark Context
conf = SparkConf().setAppName("Day2-Partitions").setMaster("local[*]")
sc = SparkContext(conf=conf)

print("=" * 70)
print("UNDERSTANDING RDD PARTITIONS")
print("=" * 70)
# =====================================================
# EXPERIMENT 1: Default Partitioning
# =====================================================
print("\n[EXPERIMENT 1] Default partitioning\n")
data = list(range(1, 101)) # 1 to 100
rdd = sc.parallelize(data)
print(f"Data size: {len(data)} elements")
print(f"Number of partitions: {rdd.getNumPartitions()}")
print(f"Default parallelism: {sc.defaultParallelism}")
# Check partition distribution
def print_partition_info(index, iterator):
    items = list(iterator)
    yield f"Partition {index}: {len(items)} elements - {items[:5]}..."

partition_info = rdd.mapPartitionsWithIndex(print_partition_info).collect()
for info in partition_info:
    print(f" {info}")
# =====================================================
# EXPERIMENT 2: Custom Partitioning
# =====================================================
print("\n[EXPERIMENT 2] Custom number of partitions\n")
# Create with specific partition count
rdd_2 = sc.parallelize(data, numSlices=2)
rdd_4 = sc.parallelize(data, numSlices=4)
rdd_10 = sc.parallelize(data, numSlices=10)
print(f"2 partitions: {rdd_2.getNumPartitions()} partitions")
print(f"4 partitions: {rdd_4.getNumPartitions()} partitions")
print(f"10 partitions: {rdd_10.getNumPartitions()} partitions")
# Show distribution for 4 partitions
print("\nDistribution with 4 partitions:")
partition_info = rdd_4.mapPartitionsWithIndex(print_partition_info).collect()
for info in partition_info:
    print(f" {info}")

# =====================================================
# EXPERIMENT 3: Repartitioning
# =====================================================
print("\n[EXPERIMENT 3] Repartitioning existing RDD\n")
print(f"Original partitions: {rdd_4.getNumPartitions()}")
# Increase partitions (repartition - triggers shuffle)
rdd_more = rdd_4.repartition(8)
print(f"After repartition(8): {rdd_more.getNumPartitions()}")
# Decrease partitions (coalesce - avoids shuffle if possible)
rdd_fewer = rdd_4.coalesce(2)
print(f"After coalesce(2): {rdd_fewer.getNumPartitions()}")
# =====================================================
# EXPERIMENT 4: Partitioning with Files
# =====================================================
print("\n[EXPERIMENT 4] Partitioning from files\n")
# Create a text file
with open("/tmp/sample_data.txt", "w") as f:
    for i in range(1, 1001):
        f.write(f"Line {i}: Some data here\n")
file_rdd = sc.textFile("/tmp/sample_data.txt")
print(f"Text file partitions: {file_rdd.getNumPartitions()}")
print(f"Note: Spark uses minimum of (file_size/128MB, 2) partitions")
# With custom min partitions
file_rdd_custom = sc.textFile("/tmp/sample_data.txt", minPartitions=4)
print(f"With minPartitions=4: {file_rdd_custom.getNumPartitions()}")
# =====================================================
# EXPERIMENT 5: Performance Impact
# =====================================================
print("\n[EXPERIMENT 5] Performance impact of partitioning\n")
import time
large_data = list(range(1000000))

# Test with different partition counts
for num_partitions in [1, 2, 4, 8, 16]:
    test_rdd = sc.parallelize(large_data, numSlices=num_partitions)
    start = time.time()
    result = test_rdd.map(lambda x: x * 2).reduce(lambda a, b: a + b)
    duration = time.time() - start
    print(f"Partitions: {num_partitions:2d} | Time: {duration:.4f}s")

print("\nObservation: Too few = underutilized, too many = overhead")

# =====================================================
# KEY TAKEAWAYS
# =====================================================
print("\n" + "=" * 70)
print("KEY TAKEAWAYS")
print("=" * 70)
print("""
1. Default partitions = number of cores in local mode
2. More partitions = more parallelism (up to a point)
3. Too many partitions = coordination overhead
4. Too few partitions = underutilized cluster
5. Rule of thumb: 2-4 partitions per CPU core
6. repartition() triggers shuffle (expensive)
7. coalesce() can avoid shuffle when reducing partitions
8. File partitions based on splits (typically 128MB)
""")
sc.stop()
