from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("Write CSV Example") \
    .getOrCreate()

# Example DataFrame
data = [
    ("Alice", 30),
    ("Bob", 25),
    ("Charlie", 35)
]

df = spark.createDataFrame(data, ["name", "age"])

# Write to CSV
df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("./out/")

spark.stop()

