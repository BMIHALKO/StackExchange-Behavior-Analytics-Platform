from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ShowEventsByType").getOrCreate()

df = spark.read.parquet("data/transformed/events_by_type_daily")

print("\nSchema:")
df.printSchema()

print("\nData:")
df.show(20, truncate = False)

spark.stop()