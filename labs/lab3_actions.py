from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder.master("local[*]")
    .appName("Day1-Actions")
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
)


customers = spark.read.option("header", "true").csv(
    "./spark-data/ecommerce/customers.csv"
)

print("=== Action 1 — count() ===")

total_count = customers.count()
print(f"Total customers: {total_count}")


enterprise_count = customers.filter(F.col("customerType") == "Enterprise").count()
print(f"Enterprise customers: {enterprise_count}")

print("=" * 20)

print("=== Action 2 — show() ===")

print("Default show() (20 rows, truncated):")
customers.show()

print("show(5, truncate=False):")
customers.show(5, truncate=False)

print("show(3, vertical=True):")
customers.show(3, vertical=True)

print("=" * 20)

print("=== Action 3 — collect() ===")

small_sample = customers.limit(3).collect()

print("Warning: collect() brings all data to driver. Only safe on small datasets!\n")
print(f"Type of small_sample: {type(small_sample)}")
print(f"Type of first element: {type(small_sample[0])}")
print("Sample values:")
for row in small_sample:
    print(f"customerName={row['customerName']}, country={row['country']}")

print("=" * 20)

print("=== Action 4 — take(n) ===")
sample_take = customers.take(5)
for row in sample_take:
    print(f"{row['customerName']} ({row['country']})")

print("=" * 20)

print("=== Action 5 — first() and head() ===")
first_row = customers.first()
head_row = customers.head()
print(f"first() customerName: {first_row['customerName']}")
print(f"head() customerName: {head_row['customerName']}")
print("Note: first() and head() return the same first row of the DataFrame.")


print("=" * 20)

print("\n=== Action 6 — write ===")

usa_customers = customers.filter(F.col("country") == "USA")
usa_customers.coalesce(1).write.mode("overwrite").option("header", True).csv(
    "./spark-data/ecommerce/usa_customers"
)
print("USA customers written as single CSV (overwrite mode).")

customers.write.mode("overwrite").parquet("./spark-data/ecommerce/customers_parquet")
print("All customers written as Parquet (overwrite mode).")


print("=" * 20)


print("=== Action 7 — foreachPartition() ===")


def partition_size(iterator):
    count = sum(1 for _ in iterator)
    print(f"Partition size: {count} rows")


customers.foreachPartition(partition_size)
print("foreachPartition executed. Partition sizes printed (may interleave in console).")





spark.stop()
