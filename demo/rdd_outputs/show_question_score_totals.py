from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ShowRDDOutput").getOrCreate()

rdd = spark.sparkContext.textFile("data/transformed/question_score_totals_rdd")

parsed = rdd.map(lambda x: eval(x))

top_scores = parsed.takeOrdered(20, key = lambda x: -x[1])

print("\nRDD Output:")
for row in top_scores:
    print(row)

spark.stop()