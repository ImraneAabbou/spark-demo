from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder.master("local[*]")
    .appName("Day1-Transformations")
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
)


df_customers = spark.read.option("header", "true").csv(
    "./spark-data/ecommerce/customers.csv"
)

print("Transformation 1: Filter 'Enterprise' → Defined, but NOT executed yet")
df_filtered = df_customers.filter(df_customers.customerType == "Enterprise")

print("Transformation 2: Select subset of columns → Defined, but NOT executed yet")
df_selected = df_filtered.select("customerName", "phone", "city", "country")

print("Transformation 3: Add column customerNameUpper → Defined, but NOT executed yet")
df_final = df_selected.withColumn(
    "customerNameUpper", F.upper(df_selected.customerName)
)

print("Triggering three actions on the final DataFrame...")
print("count():", df_final.count())

print("show(5, truncate=False):")
df_final.show(5, truncate=False)

print("limit(5).collect()", df_final.limit(5).collect())


print(
    "Summary:",
    """
What a transformation is: transformation is a logical step or layer that are stacked with no execution. until an action is called it performs all the previous transformations and this manner called lazy as we only execute the whole process only when we need it avoiding calculation redundancy and have a control over the transformations layers/steps for optimization algorithms in libraries or frameworks.
""",
)


print(
    "Task A.2:",
    """
How many jobs are triggered: 5 jobs are triggered in this scirpt, while the first job is for internal needed setup for our spark session & context, the remaining 4 jobs are for the actions ( count , show , collect ) knowing that the show actions uses internally another action which makes it in total 4 triggered jobs.
    """,
)

input("Click enter to continue...")


spark.stop()
