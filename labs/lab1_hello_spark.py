from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder.master("local[*]")
    .appName("Lab1_hello_spark")
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
)

print(spark.sparkContext._conf.getAll())

data = [
    ("Sales", 1, 100.00),
    ("Sales", 2, 150.00),
    ("Engineering", 3, 200.00),
    ("Engineering", 4, 120.00),
    ("HR", 5, 180.00),
    ("Sales", 6, 90.00),
]
columns = ["Department", "ID", "Amount"]

df = spark.createDataFrame(data, columns)

df.printSchema()


df.show(5)

df.groupBy("Department").agg(
    F.sum("Amount").alias("Total_Amount"),
    F.avg("Amount").alias("Avg_Amount"),
    F.count("ID").alias("Num_Employees"),
).show()

df.filter(df["Amount"] > 150.00).show()


spark.stop()
