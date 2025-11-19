from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("local[*]")
    .appName("test")
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
)
print("Spark version:", spark.version)

spark.stop()
