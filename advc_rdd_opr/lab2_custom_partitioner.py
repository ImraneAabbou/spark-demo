from pyspark import SparkContext, SparkConf
import time

conf = SparkConf().setAppName("Day2-CustomPartitioner").setMaster("local[*]")
sc = SparkContext(conf=conf)

print("=" * 70)
print("CUSTOM PARTITIONERS")
print("=" * 70)

# =====================================================
# LOAD CUSTOMERS
# =====================================================

customers = sc.textFile("./spark-data/ecommerce/customers.csv")
header = customers.first()
customers_data = customers.filter(lambda line: line != header)


def parse_customer(line):
    fields = line.split(",")
    return (
        int(fields[0]),  # customer_id as key
        {"name": fields[1], "country": fields[8], "segment": fields[10]},
    )


customers_rdd = customers_data.map(parse_customer)

# =====================================================
# DEFAULT HASH PARTITIONING
# =====================================================
print("\n[DEFAULT] Hash-based partitioning\n")
print(f"Number of partitions: {customers_rdd.getNumPartitions()}")


def count_per_partition(index, iterator):
    count = sum(1 for _ in iterator)
    yield (index, count)


distribution = customers_rdd.mapPartitionsWithIndex(count_per_partition).collect()

print("Default partition distribution:")
for partition_id, count in distribution:
    print(f" Partition {partition_id}: {count} records")

# =====================================================
# CUSTOM PARTITIONER 1 — COUNTRY-BASED
# =====================================================

print("\n[CUSTOM 1] Country-based partitioner\n")


class CountryPartitioner:
    """Partition data by country"""

    def __init__(self, num_partitions, countries):
        self.num_partitions = num_partitions
        self.countries = sorted(list(countries))
        self.country_map = {
            country: i % num_partitions for i, country in enumerate(self.countries)
        }

    def numPartitions(self):
        return self.num_partitions

    def getPartition(self, key):
        return self.country_map.get(key, 0)


unique_countries = customers_rdd.map(lambda x: x[1]["country"]).distinct().collect()

print(f"Unique countries: {len(unique_countries)}")
print(f"Countries sample: {sorted(unique_countries)[:10]}...")

country_pairs = customers_rdd.map(lambda x: (x[1]["country"], x))

country_partitioner = CountryPartitioner(4, unique_countries)

country_partitioned = country_pairs.partitionBy(
    numPartitions=4,
    partitionFunc=lambda country: country_partitioner.getPartition(country),
)


def show_partition_countries(index, iterator):
    countries = {}
    for country, _ in iterator:
        countries[country] = countries.get(country, 0) + 1
    yield (index, countries)


partition_info = country_partitioned.mapPartitionsWithIndex(
    show_partition_countries
).collect()

print("\nCountry partition distribution:")
for partition_id, country_counts in partition_info:
    print(f"\n Partition {partition_id}:")
    for country, count in sorted(country_counts.items())[:5]:
        print(f"  {country}: {count} customers")

# =====================================================
# CUSTOM PARTITIONER 2 — SEGMENT-BASED
# =====================================================

print("\n[CUSTOM 2] Segment-based partitioner\n")


class SegmentPartitioner:
    """Partition by customer segment"""

    def __init__(self):
        self.segment_map = {"Enterprise": 0, "Premium": 1, "Regular": 2}

    def numPartitions(self):
        return 3

    def getPartition(self, key):
        return self.segment_map.get(key, 2)


segment_pairs = customers_rdd.map(lambda x: (x[1]["segment"], x))

segment_partitioner = SegmentPartitioner()

segment_partitioned = segment_pairs.partitionBy(
    numPartitions=3, partitionFunc=lambda seg: segment_partitioner.getPartition(seg)
)


def show_partition_segments(index, iterator):
    segments = {}
    for segment, _ in iterator:
        segments[segment] = segments.get(segment, 0) + 1
    yield (index, segments)


segment_info = segment_partitioned.mapPartitionsWithIndex(
    show_partition_segments
).collect()

print("Segment partition distribution:")
for partition_id, segment_counts in segment_info:
    print(f"\n Partition {partition_id}:")
    for segment, count in sorted(segment_counts.items()):
        print(f"  {segment}: {count} customers")

# =====================================================
# CUSTOM PARTITIONER 3 — RANGE-BASED (ID)
# =====================================================

print("\n[CUSTOM 3] Range-based partitioner (by customer ID)\n")


class RangePartitioner:
    """Partition by customer ID ranges"""

    def __init__(self, ranges):
        self.ranges = ranges

    def numPartitions(self):
        return len(self.ranges)

    def getPartition(self, key):
        for i, (start, end) in enumerate(self.ranges):
            if start <= key <= end:
                return i
        return 0  # default


ranges = [(1, 250), (251, 500), (501, 750), (751, 1000)]

range_partitioner = RangePartitioner(ranges)

range_partitioned = customers_rdd.partitionBy(
    numPartitions=4, partitionFunc=lambda cid: range_partitioner.getPartition(cid)
)

range_distribution = range_partitioned.mapPartitionsWithIndex(
    count_per_partition
).collect()

print("Range-based partition distribution:")
for partition_id, count in range_distribution:
    start, end = ranges[partition_id]
    print(f" Partition {partition_id} (ID {start}-{end}): {count} customers")

# =====================================================
# BENEFIT — JOIN OPTIMIZATION THROUGH CO-PARTITIONING
# =====================================================

print("\n[BENEFIT] Co-partitioning for efficient joins\n")

orders = sc.textFile("./spark-data/ecommerce/orders.csv")
header_orders = orders.first()
orders_data = orders.filter(lambda line: line != header_orders)

orders_rdd = orders_data.map(
    lambda line: (
        int(line.split(",")[4]),  # customer_id
        {"order_id": int(line.split(",")[0]), "amount": float(line.split(",")[-2])},
    )
)

orders_partitioned = orders_rdd.partitionBy(
    numPartitions=4, partitionFunc=lambda cid: range_partitioner.getPartition(cid)
)

print("Joining co-partitioned RDDs...")
start = time.time()
joined = range_partitioned.join(orders_partitioned)
join_time = time.time() - start

print(f"Join completed in {join_time:.4f}s")
print(f"Joined records: {joined.count()}")
print("Co-partitioning eliminates shuffle in joins!")

# =====================================================
# WHEN TO USE CUSTOM PARTITIONERS
# =====================================================

print("\n" + "=" * 70)
print("WHEN TO USE CUSTOM PARTITIONERS")
print("=" * 70)

print(
    """
Use custom partitioners when:

1. Geographic analysis → partition by country  
2. Time-series data → partition by date ranges  
3. Hierarchical data → partition by category  
4. Skew mitigation → distribute hot keys  
5. Join optimization → co-partition datasets  

Benefits:
✓ Less shuffle  
✓ Faster joins  
✓ Better parallelism  
✓ Predictable data placement  
"""
)

# =====================================================
# PRACTICE EXERCISES
# =====================================================

print("\n" + "=" * 70)
print("PRACTICE EXERCISES")
print("=" * 70)

print(
    """
1. Partition by first letter of customer name  
2. Partition orders by amount ranges  
3. Compare join performance with vs without co-partitioning  
4. Create composite partitioner (country + segment)  
5. Document best use cases of custom partitioning  
"""
)


# =====================================================
# PRACTICE EXERCISES SOLUTIONS
# =====================================================

print("\n" + "=" * 70)
print("PRACTICE EXERCISES SOLUTIONS")
print("=" * 70)

## 1. Partition by first letter of customer name

print("\n[EXERCISE 1] Partition by First Letter of Name\n")


class NameInitialPartitioner:
    """Partitions based on the first letter of the customer's name."""

    def __init__(self, num_partitions):
        self.num_partitions = num_partitions
        # Create a simple mapping for the first 26 partitions (A-Z)
        self.initial_map = {chr(65 + i): i % num_partitions for i in range(26)}

    def numPartitions(self):
        return self.num_partitions

    def getPartition(self, key):
        # Key is the first letter (e.g., 'A', 'B')
        if not key:
            return 0
        initial = key[0].upper()
        # Fallback for non-alphabetic keys or keys outside the first 26
        return self.initial_map.get(initial, 0)


# Key the RDD by the first letter of the customer's name
name_initial_pairs = customers_rdd.map(lambda x: (x[1]["name"][0], x))

initial_partitioner = NameInitialPartitioner(4)

initial_partitioned = name_initial_pairs.partitionBy(
    numPartitions=4,
    partitionFunc=lambda initial: initial_partitioner.getPartition(initial),
)

initial_distribution = initial_partitioned.mapPartitionsWithIndex(
    count_per_partition
).collect()

print("Name Initial partition distribution:")
for partition_id, count in initial_distribution:
    print(f" Partition {partition_id}: {count} customers")

## 2. Partition orders by amount ranges

print("\n[EXERCISE 2] Partition Orders by Amount Ranges\n")


class OrderAmountPartitioner:
    """Partitions based on the order amount ranges."""

    def __init__(self, ranges):
        self.ranges = ranges

    def numPartitions(self):
        return len(self.ranges)

    def getPartition(self, key):
        # Key is the order amount (float)
        for i, (start, end) in enumerate(self.ranges):
            if start <= key <= end:
                return i
        return 0  # Default to partition 0


# Adjust ranges for order amounts
amount_ranges = [(0, 2500.0), (2500.01, 5000.0), (5000.01, 7000.0), (7000.01, float("inf"))]

amount_partitioner = OrderAmountPartitioner(amount_ranges)

# RDD keyed by order amount: (amount, order_record)
orders_amount_pairs = orders_rdd.map(lambda x: (x[1]["amount"], x))

orders_by_amount_partitioned = orders_amount_pairs.partitionBy(
    numPartitions=4,
    partitionFunc=lambda amount: amount_partitioner.getPartition(amount),
)

amount_distribution = orders_by_amount_partitioned.mapPartitionsWithIndex(
    count_per_partition
).collect()

print("Order Amount Range partition distribution:")
for partition_id, count in amount_distribution:
    start, end = amount_ranges[partition_id]
    print(f" Partition {partition_id} (Amount ${start}-{end}): {count} orders")

## 3. Compare join performance with vs without co-partitioning

print("\n[EXERCISE 3] Join Performance Comparison\n")

# --- Performance WITH Co-Partitioning (Already done above, re-running for clarity) ---
start_co_partitioned = time.time()
joined_co_partitioned = range_partitioned.join(orders_partitioned)
joined_co_partitioned.count()  # Trigger action
time_co_partitioned = time.time() - start_co_partitioned

# --- Performance WITHOUT Co-Partitioning (Standard Hash Join) ---
# Use the non-partitioned RDDs for the join
print("Joining NON-co-partitioned RDDs...")
start_standard = time.time()
# The join method on non-partitioned RDDs will trigger a full shuffle
joined_standard = customers_rdd.join(orders_rdd)
joined_standard.count()  # Trigger action
time_standard = time.time() - start_standard

print(f"\nJoin Time (WITH Co-Partitioning): {time_co_partitioned:.4f}s (No Shuffle)")
print(f"Join Time (WITHOUT Co-Partitioning): {time_standard:.4f}s (Full Shuffle)")
print("Difference highlights the benefit of eliminating data movement!")

## 4. Create composite partitioner (country + segment)

print("\n[EXERCISE 4] Composite Partitioner (Country + Segment)\n")


class CompositePartitioner:
    """Partitions based on a combination of country and segment."""

    def __init__(self, num_partitions, countries, segments):
        self.num_partitions = num_partitions
        self.segments = segments
        self.country_map = {
            country: i for i, country in enumerate(sorted(list(countries)))
        }

    def numPartitions(self):
        return self.num_partitions

    def getPartition(self, key):
        # Key is a tuple: (country, segment)
        country, segment = key

        # 1. Get Country Index (e.g., 0 to N_countries-1)
        country_index = self.country_map.get(country, 0)

        # 2. Get Segment Index (0, 1, or 2)
        segment_index = {"Enterprise": 0, "Premium": 1, "Regular": 2}.get(segment, 2)

        # 3. Create a unique, linear index from the composite key
        # Assuming there are 3 segments, index = CountryIndex * 3 + SegmentIndex
        composite_index = country_index * len(self.segments) + segment_index

        # 4. Map the composite index to the final number of partitions
        return composite_index % self.num_partitions


# Determine the unique segments
unique_segments = ["Enterprise", "Premium", "Regular"]

composite_partitioner = CompositePartitioner(4, unique_countries, unique_segments)

# Key the RDD by the composite key: ((country, segment), customer_record)
composite_pairs = customers_rdd.map(lambda x: ((x[1]["country"], x[1]["segment"]), x))

composite_partitioned = composite_pairs.partitionBy(
    numPartitions=4,
    partitionFunc=lambda comp_key: composite_partitioner.getPartition(comp_key),
)


# Analyze the distribution of the composite partitions
def show_composite_partition(index, iterator):
    keys = {}
    for (country, segment), _ in iterator:
        keys[(country, segment)] = keys.get((country, segment), 0) + 1
    yield (index, keys)


composite_info = composite_partitioned.mapPartitionsWithIndex(
    show_composite_partition
).collect()

print("Composite partition distribution (Partition ID: Sample Keys):")
for partition_id, key_counts in composite_info:
    print(f"\n Partition {partition_id}:")
    for (country, segment), count in sorted(key_counts.items())[:3]:
        print(f"  ({country}, {segment}): {count} customers")

## 5. Document best use cases of custom partitioning

print("\n" + "=" * 70)
print("BEST USE CASES OF CUSTOM PARTITIONING (EXERCISE 5)")
print("=" * 70)

print(
    """
The best use cases for custom partitioners revolve around **optimizing performance** and **aligning data structure** for subsequent processing steps.

1.  **Join Optimization (Co-partitioning):**
    * **Goal:** Eliminate the expensive shuffle phase during a `join` operation.
    * **Method:** Apply the *exact same* custom partitioner to **both** RDDs involved in the join (e.g., partitioning both Customers and Orders by Customer ID range). This ensures matching keys reside on the same worker node. 

2.  **Handling Data Skew:**
    * **Goal:** Prevent 'hot keys' (keys with many records) from overloading a single partition, which cripples parallel performance.
    * **Method:** For heavily skewed keys (e.g., 'USA' in country data), a custom partitioner can strategically assign these hot keys to multiple partitions, distributing the workload.

3.  **Range-based Processing:**
    * **Goal:** Group sequential or ordered data, such as time series or customer IDs, without unnecessary data movement.
    * **Method:** A `RangePartitioner` ensures all data for a specific date or ID range lands together, perfect for subsequent range queries or aggregations within a partition.

4.  **Application Logic Grouping:**
    * **Goal:** Ensure all related data needed for a specific business process is locally available (e.g., all data for 'Enterprise' segment customers).
    * **Method:** Partition by business-critical categories (like segment, product category, or sales region) to optimize subsequent map or reduce operations on that category.
"""
)

sc.stop()
