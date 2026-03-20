from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ShowTopQuestions").getOrCreate()

df = spark.read.parquet("data/transformed/top_questions_daily")

print("\nSchema:")
df.printSchema()

print("\nData:")
df.show(20, truncate = False)

spark.stop()