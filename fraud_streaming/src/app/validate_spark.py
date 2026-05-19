from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestApp") \
    .master("local[*]") \
    .getOrCreate()

# Simple job
df = spark.range(10)
print(df.count())