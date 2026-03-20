from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ShowAnsweredRate").getOrCreate()

df = spark.read.parquet("data/transformed/answered_rate_daily")

print("\nSchema:")
df.printSchema()

print("\nData:")
df.show(20, truncate = False)

spark.stop()