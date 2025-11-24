from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder.master("local[*]")
    .appName("Day1-TransformationPractice")
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
)


df_orders = spark.read.option("header", "true").csv(
    "./spark-data/ecommerce/orders.csv", inferSchema=True
)


high_values = df_orders.filter(df_orders.totalAmount > 200)

print("High values:", high_values.count())
high_values.show(5)


# Renaming Cols

order_summary = (
    df_orders.withColumnRenamed("orderNumber", "id")
    .withColumnRenamed("orderNumber", "date")
    .withColumnRenamed("totalAmount", "amount")
)

order_summary.show(5)

orders_categorized = df_orders.withColumn(
    "orderSize",
    F.when(F.col("totalAmount") < 1000, "Small")
    .when((F.col("totalAmount") >= 1000) & (F.col("totalAmount") <= 4999), "Medium")
    .otherwise("Large"),
)

orders_categorized.groupBy("orderSize").count().orderBy("orderSize")


processed_orders = df_orders.filter(F.col("status") == "SHIPPED")

processed_orders = processed_orders.withColumn(
    "", F.round(F.col("totalAmount"), 0)
).withColumn(
    "priority",
    F.when(F.col("totalAmount") > 500, "HIGH")
    .when(F.col("totalAmount") > 500, "MEDIUM")
    .otherwise("LOW"),
)

processed_orders.show(10)

processed_orders_dropped_cols = (
    processed_orders
    # Dropping few cols
    .drop("status").drop("orderDate")
)


print(
    f"Cols before removing is {len(processed_orders.columns)} after removing bcmz {len(processed_orders_dropped_cols.columns)}"
)


# TASK B.2 — Implement “Your Turn” Practice Questions
df_orders = df_orders.withColumn(
    "totalAmount", F.col("totalAmount").cast("float")
).withColumn("orderDate", F.col("orderDate").cast("date"))

df_filtered = df_orders.filter(F.col("orderDate") >= F.lit("2024-06-01"))

df_filtered = df_filtered.withColumn("isLargeOrder", F.col("totalAmount") > F.lit(300))

df_filtered = df_filtered.filter(F.col("status").isin("PENDING", "SHIPPED"))

df_filtered = df_filtered.withColumn(
    "orderCode",
    F.concat(
        F.lit("ORDER-"),
        F.lpad(F.col("orderNumber"), 5, "0")
    )
)

df_top10 = df_filtered.orderBy(F.col("totalAmount").desc()).limit(10)

df_top10.show()




spark.stop()
