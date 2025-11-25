from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Day2-Joins").setMaster("spark://spark-master:7077")
sc = SparkContext(conf=conf)

print("=" * 70)
print("JOIN OPERATIONS")
print("=" * 70)


# =====================================================
# LOAD AND PREPARE DATA
# =====================================================
def load_customers():
    customers = sc.textFile("./spark-data/ecommerce/customers.csv")
    header = customers.first()
    data = customers.filter(lambda line: line != header)

    def parse(line):
        fields = line.split(",")
        return (
            int(fields[0]),  # Key: customer_id
            {
                "name": fields[1],
                "city": fields[6],
                "country": fields[8],
                "segment": fields[10],
            },
        )

    return data.map(parse)


def load_orders():
    orders = sc.textFile("./spark-data/ecommerce/orders.csv")
    header = orders.first()
    data = orders.filter(lambda line: line != header)

    def parse(line):
        fields = line.split(",")
        return (
            int(fields[4]),  # Key: customer_id
            {
                "order_id": int(fields[0]),
                "order_date": fields[1],
                "status": fields[3],
                "amount": float(fields[5]),
            },
        )

    return data.map(parse)


customers = load_customers()
orders = load_orders()
print(f"Customers: {customers.count()}")
print(f"Orders: {orders.count()}")

# =====================================================
# JOIN 1: Inner Join
# =====================================================
print("\n[JOIN 1] Inner Join - Orders with customer info\n")
joined = customers.join(orders)
# Result: (customer_id, (customer_dict, order_dict))
print("Sample joined records:")
for customer_id, (cust, order) in joined.take(5):
    print(
        f" Customer {customer_id}: {cust['name']:30s} | "
        f"Order #{order['order_id']}: ${order['amount']:.2f}"
    )
print(f"\nTotal joined records: {joined.count()}")
print("Note: Only customers WITH orders are included")

# =====================================================
# JOIN 2: Left Outer Join
# =====================================================
print("\n[JOIN 2] Left Outer Join - All customers, with/without orders\n")
left_joined = customers.leftOuterJoin(orders)
# Count customers without orders
customers_without_orders = left_joined.filter(lambda x: x[1][1] is None)
customers_with_orders = left_joined.filter(lambda x: x[1][1] is not None)
print(f"Customers WITH orders: {customers_with_orders.count()}")
print(f"Customers WITHOUT orders: {customers_without_orders.count()}")
print("\nSample customers without orders:")
for customer_id, (cust, order) in customers_without_orders.take(5):
    print(f" Customer {customer_id}: {cust['name']:30s} from {cust['city']}")

# =====================================================
# JOIN 3: Right Outer Join
# =====================================================
print("\n[JOIN 3] Right Outer Join - All orders, even without customer data\n")
right_joined = orders.rightOuterJoin(customers)
print(f"Total records: {right_joined.count()}")
orphaned_orders = right_joined.filter(lambda x: x[1][0] is None)
print(f"Orphaned orders (no customer): {orphaned_orders.count()}")

# =====================================================
# JOIN 4: Full Outer Join
# =====================================================
print("\n[JOIN 4] Full Outer Join - Everything\n")
full_joined = customers.fullOuterJoin(orders)
both_present = full_joined.filter(lambda x: x[1][0] is not None and x[1][1] is not None)
only_customer = full_joined.filter(lambda x: x[1][0] is not None and x[1][1] is None)
only_order = full_joined.filter(lambda x: x[1][0] is None and x[1][1] is not None)
print(f"Records with both customer and order: {both_present.count()}")
print(f"Records with only customer: {only_customer.count()}")
print(f"Records with only order: {only_order.count()}")

# =====================================================
# JOIN 5: Cartesian Join (BE CAREFUL!)
# =====================================================
print("\n[JOIN 5] Cartesian Join - All combinations\n")
sample_customers = customers.take(3)
sample_orders = orders.take(3)
small_customers = sc.parallelize(sample_customers)
small_orders = sc.parallelize(sample_orders)
cartesian = small_customers.cartesian(small_orders)
print(f"Cartesian result size: {cartesian.count()}")
print("(3 customers × 3 orders = 9 combinations)")
print("\nSample combinations:")
for (cid, cust), (oid, order) in cartesian.take(3):
    print(f" Customer {cid} + Order from customer {oid}")
print("\n WARNING: Cartesian joins are EXTREMELY EXPENSIVE!")
print("Use only on very small datasets or with filters")

# =====================================================
# JOIN 6: Self Join
# =====================================================
print("\n[JOIN 6] Self Join - Find customers in same city\n")
customers_by_city = customers.map(lambda x: (x[1]["city"], (x[0], x[1]["name"])))
same_city = customers_by_city.join(customers_by_city)
same_city_filtered = same_city.filter(lambda x: x[1][0][0] < x[1][1][0])
print("Sample customers in same city:")
for city, ((id1, name1), (id2, name2)) in same_city_filtered.take(5):
    print(f" {city}: {name1} and {name2}")

# =====================================================
# JOIN 7: Broadcast Join (Small + Large)
# =====================================================
print("\n[JOIN 7] Broadcast join optimization\n")
status_descriptions = sc.parallelize(
    [
        ("Shipped", "Order has been shipped to customer"),
        ("Processing", "Order is being processed"),
        ("On Hold", "Order is temporarily on hold"),
        ("Cancelled", "Order was cancelled"),
    ]
)
status_map = sc.broadcast(dict(status_descriptions.collect()))
print(f"Broadcasted status map size: {len(status_map.value)} entries")

orders_with_desc = orders.mapValues(
    lambda o: {**o, "status_desc": status_map.value.get(o["status"], "Unknown")}
)
print("\nSample orders with descriptions:")
for customer_id, order in orders_with_desc.take(5):
    print(
        f" Order #{order['order_id']}: {order['status']:12s} - {order['status_desc']}"
    )
print("\n Broadcast join avoids shuffle (much faster for small datasets!)")

# =====================================================
# JOIN 8: Multi-way Join
# =====================================================
print("\n[JOIN 8] Three-way join example\n")
payments = sc.parallelize(
    [(1, "Credit Card"), (2, "PayPal"), (3, "Bank Transfer"), (4, "Cash")]
)
order_payments = orders.map(lambda x: (x[1]["order_id"], x[0]))  # simplified
print("Multi-way joins involve chaining multiple join operations")
print("Example: orders -> customers -> payment_methods")

# =====================================================
# PRACTICAL EXAMPLE: Customer Order Summary
# =====================================================
print("\n" + "=" * 70)
print("PRACTICAL EXAMPLE: Complete Customer Analysis")
print("=" * 70)

customer_order_summary = (
    customers.join(orders)
    .map(lambda x: (x[0], (x[1][0], x[1][1]["amount"])))
    .aggregateByKey(
        {"customer_name": None, "total": 0, "count": 0, "city": None, "segment": None},
        lambda acc, val: {
            "customer_name": val[0]["name"],
            "city": val[0]["city"],
            "segment": val[0]["segment"],
            "total": acc["total"] + val[1],
            "count": acc["count"] + 1,
        },
        lambda acc1, acc2: {
            "customer_name": acc1["customer_name"] or acc2["customer_name"],
            "city": acc1["city"] or acc2["city"],
            "segment": acc1["segment"] or acc2["segment"],
            "total": acc1["total"] + acc2["total"],
            "count": acc1["count"] + acc2["count"],
        },
    )
    .mapValues(
        lambda x: {**x, "average": x["total"] / x["count"] if x["count"] > 0 else 0}
    )
)

print("\nTop 10 customers by total spending:")
top_10 = customer_order_summary.sortBy(lambda x: x[1]["total"], ascending=False).take(
    10
)
for customer_id, stats in top_10:
    print(
        f" {stats['customer_name']:30s} | {stats['city']:15s} | "
        f"{stats['segment']:10s} | {stats['count']:3d} orders | "
        f"${stats['total']:>10,.2f} total | ${stats['average']:>8,.2f} average"
    )

# =====================================================
# PRACTICE EXERCISES
# =====================================================
print("\n" + "=" * 70)
print("PRACTICE EXERCISES")
print("=" * 70)
print(
    """
Complete these exercises:

Run the script:

python lab2_joins.py

Practice Solutions

Add these after the practice exercises section:
1. Find customers with more than 5 orders
2. Calculate total revenue by country (join required)
3. Find customers in USA with total spending > $10,000
4. Identify the top city by total order volume
5. Find customers who have never placed an order (use left outer join)
"""
)


# =====================================================
# PRACTICE SOLUTIONS
# =====================================================

print("\n" + "=" * 70)
print("PRACTICE SOLUTIONS")
print("=" * 70)

# Solution 1: Customers with more than 5 orders
orders_per_customer = orders.countByKey()
customers_5plus = [
    (cid, count) for cid, count in orders_per_customer.items() if count > 5
]
print(f"\nSolution 1: {len(customers_5plus)} customers with >5 orders")
print(f"Sample: {customers_5plus[:5]}")

# Solution 2: Total revenue by country
revenue_by_country = (
    customers.join(orders)
    .map(lambda x: (x[1][0]["country"], x[1][1]["amount"]))
    .reduceByKey(lambda a, b: a + b)
    .sortBy(lambda x: x[1], ascending=False)
)

print("\nSolution 2: Revenue by country (Top 5):")
for country, revenue in revenue_by_country.take(5):
    print(f" {country:15s}: ${revenue:>12,.2f}")

# Solution 3: USA customers with spending > $10,000
usa_high_spenders = (
    customers.join(orders)
    .filter(lambda x: x[1][0]["country"] == "USA")
    .map(lambda x: (x[0], (x[1][0]["name"], x[1][1]["amount"])))
    .aggregateByKey(
        {"name": None, "total": 0},
        lambda acc, val: {"name": val[0], "total": acc["total"] + val[1]},
        lambda acc1, acc2: {
            "name": acc1["name"] or acc2["name"],
            "total": acc1["total"] + acc2["total"],
        },
    )
    .filter(lambda x: x[1]["total"] > 10000)
)

print(
    f"\nSolution 3: {usa_high_spenders.count()} USA customers with >$10,000 total spending"
)

# Solution 4: Top city by order volume
city_volume = (
    customers.join(orders)
    .map(lambda x: (x[1][0]["city"], x[1][1]["amount"]))
    .reduceByKey(lambda a, b: a + b)
    .sortBy(lambda x: x[1], ascending=False)
)

top_city = city_volume.first()
print(f"\nSolution 4: Top city: {top_city[0]} with ${top_city[1]:,.2f} in total orders")

# Solution 5: Customers who never ordered
never_ordered = (
    customers.leftOuterJoin(orders)
    .filter(lambda x: x[1][1] is None)
    .map(lambda x: (x[0], x[1][0]["name"]))
)

print(f"\nSolution 5: {never_ordered.count()} customers never placed an order")
print("Sample:")
for cid, name in never_ordered.take(5):
    print(f" Customer {cid}: {name}")

sc.stop()
