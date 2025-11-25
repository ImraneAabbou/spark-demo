from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Day2-MapPractice").setMaster("local[*]")

sc = SparkContext(conf=conf)

print("=" * 70)
print("MAP OPERATIONS - PRACTICE PROBLEMS")
print("=" * 70)

# Load and parse customers
customers = sc.textFile("spark-data/ecommerce/customers.csv")
header = customers.first()
customers_data = customers.filter(lambda line: line != header)


def parse_customer(line):
    fields = line.split(",")
    return {
        "id": int(fields[0]),
        "name": fields[1],
        "first_name": fields[2],
        "last_name": fields[3],
        "city": fields[6],
        "country": fields[8],
        "credit_limit": float(fields[9]) if fields[9] else 0.0,
        "segment": fields[10],
    }


customers_rdd = customers_data.map(parse_customer)

# =====================================================
# PROBLEM 1: Extract USA customers
# =====================================================
print("\n[PROBLEM 1] USA Customers\n")

# TODO: Filter customers from USA
# Your code here:
usa_customers = customers_rdd.filter(lambda c: c['country'] == "USA")

# Uncomment when ready:
print(f"USA customers: {usa_customers.count()}")
print("Sample USA customers:")
for customer in usa_customers.take(5):
    print(f"  {customer['name']} from {customer['city']}")

# =====================================================
# PROBLEM 2: Create (city, country) tuples
# =====================================================
print("\n[PROBLEM 2] City-Country pairs\n")

# TODO: Create RDD of (city, country) tuples
# Your code here:
city_country = customers_rdd.map(lambda c: (c['city'], c['country']))

# Uncomment when ready:
print("Sample city-country pairs:")
for pair in city_country.take(10):
    print(f"  {pair}")

# =====================================================
# PROBLEM 3: Format customer IDs
# =====================================================
print("\n[PROBLEM 3] Formatted IDs\n")

# TODO: Create formatted IDs like "CUST-00001", "CUST-00002", etc.
# Hint: Use string formatting with zero-padding
# Your code here:
formatted_ids = customers_rdd.map(lambda c: (c['id'], f"CUST-{c['id']:05d}"))

# Uncomment when ready:
print("Sample formatted IDs:")
for customer_id, formatted in formatted_ids.take(10):
    print(f"  {customer_id} → {formatted}")

# =====================================================
# PROBLEM 4: High-value customers
# =====================================================
print("\n[PROBLEM 4] High-value customers (>$50k)\n")

# TODO: Find customers with credit limit > $50,000
# Your code here:
high_value = customers_rdd.filter(lambda c: c['credit_limit'] > 50000)

# Uncomment when ready:
print(f"High-value customers: {high_value.count()}")
print("Sample high-value customers:")
for customer in high_value.take(5):
    print(f"  {customer['name']:30s} | ${customer['credit_limit']:,.2f}")

# =====================================================
# PROBLEM 5: Customer summary with credit tier
# =====================================================
print("\n[PROBLEM 5] Create customer summary\n")

# TODO: Create tuple of (id, name, segment, credit_tier)
# Where credit_tier is:
# - "High" if credit_limit > 75000
# - "Medium" if 25000 <= credit_limit <= 75000
# - "Low" if credit_limit < 25000
# Your code here:
customer_summary = customers_rdd.map(
    lambda c: (
        c['id'],
        c['name'],
        c['segment'],
        "High" if c['credit_limit'] > 75000
        else "Medium" if c['credit_limit'] >= 25000
        else "Low"
    )
)

# Uncomment when ready:
print("Sample customer summaries:")
for cid, name, segment, tier in customer_summary.take(10):
    print(f"  {cid:4d} | {name:30s} | {segment:10s} | {tier}")

# =====================================================
# BONUS: Calculate average credit by segment
# =====================================================
print("\n[BONUS] Average credit limit by segment\n")

# TODO: Calculate average credit limit for each segment
# Hint: You'll need groupByKey() or aggregateByKey()
# Your code here:
# Create (segment, credit_limit) pairs
segment_credit = customers_rdd.map(lambda c: (c['segment'], c['credit_limit']))

# Calculate average using aggregateByKey
sum_count = segment_credit.aggregateByKey(
    (0.0, 0),               # Initial value: (sum, count)
    lambda acc, x: (acc[0] + x, acc[1] + 1),  # SeqOp
    lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])  # CombOp
)

avg_by_segment = sum_count.mapValues(lambda x: x[0] / x[1])


# Uncomment when ready:
print("Average credit limit by segment:")
for segment, avg in sorted(avg_by_segment.collect()):
    print(f"  {segment:10s}: ${avg:,.2f}")

print("\n" + "=" * 70)
print("Complete all problems and verify your solutions!")
print("=" * 70)

sc.stop()
