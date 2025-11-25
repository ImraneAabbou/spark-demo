from pyspark import SparkContext, SparkConf
from datetime import datetime

conf = SparkConf().setAppName("Day2-Aggregations").setMaster("local[*]")
sc = SparkContext(conf=conf)

print("=" * 70)
print("ADVANCED AGGREGATIONS")
print("=" * 70)

# =====================================================
# LOAD ORDERS
# =====================================================

orders = sc.textFile("spark-data/ecommerce/orders.csv")
header = orders.first()
orders_data = orders.filter(lambda line: line != header)

def parse_order(line):
    fields = line.split(',')
    return {
        'order_id': int(fields[0]),
        'order_date': datetime.strptime(fields[1], '%Y-%m-%d'),
        'status': fields[3],
        'customer_id': int(fields[4]),
        'amount': float(fields[5])
    }

orders_rdd = orders_data.map(parse_order)

# =====================================================
# AGGREGATION 1: Basic Statistics
# =====================================================
print("\n[AGGREGATION 1] Basic statistics\n")

amounts = orders_rdd.map(lambda o: o['amount'])

total = amounts.sum()
count = amounts.count()
average = amounts.mean()
min_val = amounts.min()
max_val = amounts.max()
stdev = amounts.stdev()

print(f"Total Orders: {count:,}")
print(f"Total Revenue: ${total:,.2f}")
print(f"Average Order: ${average:.2f}")
print(f"Min Order: ${min_val:.2f}")
print(f"Max Order: ${max_val:.2f}")
print(f"Std Deviation: ${stdev:.2f}")

# =====================================================
# AGGREGATION 2: Group Statistics
# =====================================================
print("\n[AGGREGATION 2] Statistics by status\n")

stats_by_status = (
    orders_rdd
        .map(lambda o: (o['status'], o['amount']))
        .aggregateByKey(
            (0, 0, float('inf'), float('-inf'), 0),  
            lambda acc, val: (
                acc[0] + val,           # sum
                acc[1] + 1,             # count
                min(acc[2], val),       # min
                max(acc[3], val),       # max
                acc[4] + val ** 2       # sum of squares
            ),
            lambda acc1, acc2: (
                acc1[0] + acc2[0],
                acc1[1] + acc2[1],
                min(acc1[2], acc2[2]),
                max(acc1[3], acc2[3]),
                acc1[4] + acc2[4]
            )
        )
        .mapValues(lambda x: {
            'count': x[1],
            'total': x[0],
            'average': x[0] / x[1],
            'min': x[2],
            'max': x[3],
            'variance': (x[4] / x[1]) - (x[0] / x[1]) ** 2
        })
)

for status, stats in sorted(stats_by_status.collect()):
    print(f"\n{status}:")
    print(f" Count: {stats['count']:,}")
    print(f" Total: ${stats['total']:,.2f}")
    print(f" Average: ${stats['average']:.2f}")
    print(f" Min: ${stats['min']:.2f}")
    print(f" Max: ${stats['max']:.2f}")
    print(f" Variance: {stats['variance']:.2f}")

# =====================================================
# AGGREGATION 3: Time-based Analysis
# =====================================================
print("\n[AGGREGATION 3] Monthly revenue trends\n")

monthly_revenue = (
    orders_rdd
        .map(lambda o: (f"{o['order_date'].year}-{o['order_date'].month:02d}", o['amount']))
        .reduceByKey(lambda a, b: a + b)
        .sortByKey()
)

print("Monthly Revenue:")
for month, revenue in monthly_revenue.collect():
    print(f" {month}: ${revenue:>10,.2f}")

# =====================================================
# AGGREGATION 4: Percentiles
# =====================================================
print("\n[AGGREGATION 4] Order amount percentiles\n")

sorted_amounts = amounts.sortBy(lambda x: x).zipWithIndex()
total_count = sorted_amounts.count()

def get_percentile(rdd, count, percentile):
    index = int(count * percentile)
    result = rdd.filter(lambda x: x[1] == index).first()
    return result[0] if result else 0

p25 = get_percentile(sorted_amounts, total_count, 0.25)
p50 = get_percentile(sorted_amounts, total_count, 0.50)
p75 = get_percentile(sorted_amounts, total_count, 0.75)
p95 = get_percentile(sorted_amounts, total_count, 0.95)

print(f"25th percentile: ${p25:.2f}")
print(f"50th percentile (median): ${p50:.2f}")
print(f"75th percentile: ${p75:.2f}")
print(f"95th percentile: ${p95:.2f}")

# =====================================================
# AGGREGATION 5: RFM Analysis
# =====================================================

print("\n[AGGREGATION 5] RFM Analysis (Recency, Frequency, Monetary)\n")

reference_date = datetime(2024, 12, 1)

rfm = (
    orders_rdd
        .map(lambda o: (o['customer_id'], (o['order_date'], o['amount'])))
        .aggregateByKey(
            (reference_date, 0, 0),
            lambda acc, val: (
                min(acc[0], val[0]) if acc[0] != reference_date else val[0],
                acc[1] + 1,
                acc[2] + val[1]
            ),
            lambda acc1, acc2: (
                min(acc1[0], acc2[0]),
                acc1[1] + acc2[1],
                acc1[2] + acc2[2]
            )
        )
        .mapValues(lambda x: {
            'recency_days': (reference_date - x[0]).days,
            'frequency': x[1],
            'monetary': x[2]
        })
)

print("Sample RFM scores:")
for customer_id, rfm_scores in rfm.take(10):
    print(f" Customer {customer_id:4d}: "
          f"R={rfm_scores['recency_days']:3d}d, "
          f"F={rfm_scores['frequency']:2d} orders, "
          f"M=${rfm_scores['monetary']:>8,.2f}")

def categorize_rfm(scores):
    r_score = 5 if scores['recency_days'] < 30 else (3 if scores['recency_days'] < 90 else 1)
    f_score = 5 if scores['frequency'] >= 10 else (3 if scores['frequency'] >= 5 else 1)
    m_score = 5 if scores['monetary'] >= 5000 else (3 if scores['monetary'] >= 1000 else 1)

    total = r_score + f_score + m_score

    if total >= 13:
        return "Champions"
    elif total >= 9:
        return "Loyal"
    elif total >= 6:
        return "Potential"
    else:
        return "At Risk"

rfm_segments = rfm.mapValues(categorize_rfm)
segment_counts = rfm_segments.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b)

print("\nRFM Segments:")
for segment, count in sorted(segment_counts.collect(), key=lambda x: x[1], reverse=True):
    print(f" {segment:15s}: {count:4d} customers")

# =====================================================
# AGGREGATION 6: Cohort Analysis
# =====================================================
print("\n[AGGREGATION 6] Cohort analysis by first purchase month\n")

first_purchase = (
    orders_rdd
        .map(lambda o: (o['customer_id'], o['order_date']))
        .reduceByKey(min)
        .mapValues(lambda d: f"{d.year}-{d.month:02d}")
)

cohort_revenue = (
    orders_rdd
        .map(lambda o: (o['customer_id'], (o['order_date'], o['amount'])))
        .join(first_purchase)
        .map(lambda x: (x[1][1], x[1][0][1]))  
        .reduceByKey(lambda a, b: a + b)
        .sortByKey()
)

print("Revenue by customer cohort (first purchase month):")
for cohort, revenue in cohort_revenue.collect():
    print(f" {cohort} cohort: ${revenue:>10,.2f}")

# =====================================================
# AGGREGATION 7: Running Totals
# =====================================================
print("\n[AGGREGATION 7] Cumulative revenue over time\n")

daily_revenue = (
    orders_rdd
        .map(lambda o: (o['order_date'].strftime('%Y-%m-%d'), o['amount']))
        .reduceByKey(lambda a, b: a + b)
        .sortByKey()
)

daily_list = daily_revenue.collect()
cumulative = 0

print("Sample cumulative revenue (first 10 days):")
for date, revenue in daily_list[:10]:
    cumulative += revenue
    print(f" {date}: ${revenue:>8,.2f} (cumulative: ${cumulative:>12,.2f})")

# =====================================================
# SUMMARY
# =====================================================

print("\n" + "=" * 70)
print("AGGREGATION SUMMARY")
print("=" * 70)

print("""
Key Aggregation Techniques:
1. Basic statistics: sum(), mean(), min(), max(), stdev()
2. Group statistics: aggregateByKey() for complex metrics
3. Time-based: Group by date/month for trends
4. Percentiles: Sort and index for distribution analysis
5. RFM: Recency, Frequency, Monetary for segmentation
6. Cohort: Track customer groups over time
7. Running totals: Accumulate metrics sequentially
""")

sc.stop()

