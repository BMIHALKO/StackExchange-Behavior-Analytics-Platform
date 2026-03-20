from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ShowRegionDistribution").getOrCreate()

df = spark.read.parquet("data/transformed/region_distribution")

print("\nSchema:")
df.printSchema()

print("\nData:")
df.show(20, truncate = False)

spark.stop()