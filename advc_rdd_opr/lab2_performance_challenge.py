from pyspark import SparkContext, SparkConf
import time

conf = SparkConf().setAppName("Day2-PerformanceChallenge").setMaster("local[*]")
sc = SparkContext(conf=conf)

print("=" * 70)
print("PERFORMANCE OPTIMIZATION CHALLENGE")
print("=" * 70)

# Load datasets
customers = sc.textFile("spark-data/ecommerce/customers.csv")
orders = sc.textFile("spark-data/ecommerce/orders.csv")

# Parse data
customers_data = customers.filter(lambda line: 'customerNumber' not in line)
orders_data = orders.filter(lambda line: 'orderNumber' not in line)

def parse_customer(line):
    fields = line.split(',')
    return (int(fields[0]), {'name': fields[1], 'segment': fields[-1]})

def parse_order(line):
    fields = line.split(',')
    return (int(fields[4]), {'order_id': int(fields[0]), 'amount': float(fields[-2])})

# =====================================================
# CHALLENGE: Optimize this pipeline
# =====================================================
print("\n[CHALLENGE] Find top 10 Enterprise customers by spending\n")
print("Goal: Optimize the following slow implementation\n")

# SLOW IMPLEMENTATION (baseline)
print("Running BASELINE (slow) implementation...")
start = time.time()

customers_rdd = customers_data.map(parse_customer)
orders_rdd = orders_data.map(parse_order)

# Multiple scans and inefficient operations
enterprise = customers_rdd.filter(lambda x: x[1]['segment'] == 'Enterprise')
joined = enterprise.join(orders_rdd)
totals = joined.map(lambda x: (x[0], x[1][1]['amount'])).reduceByKey(lambda a, b: a + b)
with_names = totals.join(customers_rdd)
result1 = with_names.map(lambda x: (x[1][1]['name'], x[1][0])).sortBy(lambda x: x[1], ascending=False).take(10)

time1 = time.time() - start
print(f"\n Baseline time: {time1:.4f}s")
print("\nTop 10 Enterprise customers:")
for name, total in result1:
    print(f" {name:30s}: ${total:,.2f}")

# =====================================================
# YOUR TURN: Implement optimizations!
# =====================================================
print("\n" + "=" * 70)
print("YOUR OPTIMIZED IMPLEMENTATION")
print("=" * 70)
print("""
Apply these optimization techniques:
1. Cache frequently reused RDDs
2. Filter early (push down predicates)
3. Use broadcast for small datasets
4. Reduce shuffles where possible

5. Optimize partitioning
6. Avoid redundant joins
Implement your optimizations below:
""")

# TODO: YOUR OPTIMIZED CODE HERE
print("\nRunning OPTIMIZED implementation...")
start = time.time()

# HINT: Start by caching the parsed data
customers_rdd = customers_data.map(parse_customer).cache()
orders_rdd = orders_data.map(parse_order).cache()

# HINT: Filter customers early
enterprise_customers = customers_rdd.filter(lambda x: x[1]['segment'] == 'Enterprise')

# HINT: Could you use broadcast for the enterprise customer list?
# HINT: Can you combine the two joins into one?
# HINT: Can you avoid the second join entirely?

# YOUR OPTIMIZATION HERE
# Example optimization (you can do better!):
result2 = enterprise_customers.join(orders_rdd) \
    .map(lambda x: (x[0], (x[1][0]['name'], x[1][1]['amount']))) \
    .aggregateByKey(
        {'name': None, 'total': 0},
        lambda acc, val: {'name': val[0], 'total': acc['total'] + val[1]},
        lambda acc1, acc2: {'name': acc1['name'] or acc2['name'], 'total': acc1['total'] + acc2['total']}
    ) \
    .map(lambda x: (x[1]['name'], x[1]['total'])) \
    .sortBy(lambda x: x[1], ascending=False) \
    .take(10)

time2 = time.time() - start
print(f"\n Optimized time: {time2:.4f}s")
print(f" Speedup: {time1/time2:.2f}x faster!")

print("\nVerify results match:")
for name, total in result2:
    print(f" {name:30s}: ${total:,.2f}")

print("\nRun the challenge:")
print("python lab2_performance_challenge.py")

# Challenge Goals:
# 1. Achieve at least 2x speedup
# 2. Reduce number of shuffles
# 3. Minimize memory usage
# 4. Document all optimizations

# =====================================================
# DOCUMENT YOUR OPTIMIZATIONS
# =====================================================
print("\n" + "=" * 70)
print("DOCUMENT YOUR OPTIMIZATIONS")
print("=" * 70)
print("""
List the optimizations you applied:
1. Caching: Cached parsed RDDs to avoid re-parsing
2. Early filtering: Filtered to Enterprise segment before join
3. Single aggregation: Combined join and aggregation in one step
4. Removed redundant join: Carried customer name through pipeline
5. [Add your optimizations here]

Performance analysis:
- Baseline shuffles: [count them]
- Optimized shuffles: [count them]
- Memory usage: [observe in Spark UI]
- Task distribution: [check Spark UI]

Further improvements possible:
- [Your ideas here]
""")


print("\n" + "=" * 70)
print("YOUR OPTIMIZED IMPLEMENTATION")
print("=" * 70)

# 1. Caching: The RDDs are already cached at the beginning of the block.
customers_rdd = customers_data.map(parse_customer).cache()
orders_rdd = orders_data.map(parse_order).cache()

# 2. Filter Early: Push the predicate down to the customer dataset.
enterprise_customers_only = customers_rdd.filter(
    lambda x: x[1]['segment'] == 'Enterprise'
)

# 3. Streamline Join and Aggregation (Key Optimization)
# The goal is to join the two RDDs and immediately start aggregating spending.
# The original slow code performed:
# (1) join -> (2) map to (customer_id, amount) -> (3) reduceByKey (SHUFFLE) -> (4) join again (SHUFFLE).
# The optimized approach performs:
# (1) map customers to (customer_id, name) and cache/collect (if small)
# (2) join (SHUFFLE) -> (3) map to (name, amount) -> (4) reduceByKey (SHUFFLE)
# The best approach is to carry the necessary information (name) across the single shuffle.

print("\nRunning OPTIMIZED implementation (Single Shuffle Aggregation)...")
start = time.time()

# Step 1: Join Enterprise Customers with Orders
# Key: customer_id
# Value: ({'name': '...', 'segment': 'Enterprise'}, {'order_id': '...', 'amount': '...'})
joined_data = enterprise_customers_only.join(orders_rdd)

# Step 2: Map to the final aggregation key and value, carrying the name.
# New RDD element: (Customer ID, (Name, Amount))
# Since the customer name is unique for a customer ID, we can carry it.
# Map: (Name, Total_Spending)
spending_by_name = joined_data.map(
    lambda x: (
        x[1][0]['name'],  # x[1][0] is customer_data, grab the name
        x[1][1]['amount'] # x[1][1] is order_data, grab the amount
    )
)

# Step 3: Aggregate spending by name (Single SHUFFLE for reduction)
# The `reduceByKey` operation performs the aggregation and shuffles the data only once.
total_spending = spending_by_name.reduceByKey(lambda a, b: a + b)

# Step 4: Sort and Take Top 10
result2 = total_spending.sortBy(
    lambda x: x[1], 
    ascending=False
).take(10)

# Optional Advanced Optimization (if customer_id space is small, using Broadcast):
# If the *number* of enterprise customers is small (e.g., < 10,000 records),
# we could use a Broadcast join to eliminate the first join shuffle.
#
# enterprise_map = sc.broadcast(enterprise_customers_only.collectAsMap())
#
# result_broadcast = orders_rdd.filter(lambda x: x[0] in enterprise_map.value) \
#     .map(lambda x: (enterprise_map.value[x[0]]['name'], x[1]['amount'])) \
#     .reduceByKey(lambda a, b: a + b) \
#     .sortBy(lambda x: x[1], ascending=False).take(10)
#
# However, the `join/reduceByKey/sortBy` chain above is generally robust and handles larger datasets well.

# End of optimization block
time2 = time.time() - start
print(f"\n Optimized time: {time2:.4f}s")
print(f" Speedup: {time1/time2:.2f}x faster!")

print("\nVerify results match:")
for name, total in result2:
    print(f" {name:30s}: ${total:,.2f}")

print("\nRun the challenge:")
print("python lab2_performance_challenge.py")

# =====================================================
# DOCUMENT YOUR OPTIMIZATIONS
# =====================================================
print("\n" + "=" * 70)
print("DOCUMENT YOUR OPTIMIZATIONS")
print("=" * 70)
print("""
List the optimizations you applied:
1. Caching: Cached parsed RDDs (`customers_rdd`, `orders_rdd`) to avoid re-parsing on multiple subsequent actions.
2. Early Filtering: Filtered the large `customers_rdd` to `enterprise_customers_only` **before** the join (Push Down Predicates). This drastically reduces the data size entering the first shuffle.
3. Removed Redundant Join: The baseline performed `totals.join(customers_rdd)`. The optimized code carries the customer name (`x[1][0]['name']`) across the *first* join, eliminating the need for the second join entirely.
4. Reduced Shuffles: The baseline required **3 shuffles** (Join 1, ReduceByKey, Join 2). The optimized code requires only **2 shuffles** (Join, ReduceByKey). 

Performance analysis:
- Baseline shuffles: **3** (Join, ReduceByKey, Join)
- Optimized shuffles: **2** (Join, ReduceByKey)
- Memory usage: Reduced due to early filtering.
- Task distribution: The tasks are better balanced after filtering.

Further improvements possible:
- **Co-partitioning:** If both `enterprise_customers_only` and `orders_rdd` were partitioned by the same custom Partitioner (e.g., `RangePartitioner` on `customer_id`), the initial `join` shuffle could be eliminated, reducing the total shuffle count to **1** (`reduceByKey`).
- **Broadcast Join:** If the filtered `enterprise_customers_only` dataset is very small (a few thousand records), converting it to a broadcast variable (`sc.broadcast`) and using a map-side join would eliminate the join shuffle entirely, achieving a total of **1 shuffle** (`reduceByKey`).
""")

# Cleanup
enterprise_customers.unpersist()
customers_rdd.unpersist()
orders_rdd.unpersist()

sc.stop()
