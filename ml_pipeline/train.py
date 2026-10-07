from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("traffic-ml").getOrCreate()

train = spark.read.parquet("s3a://traffic-clean/train")
test = spark.read.parquet("s3a://traffic-clean/test")

print("Train rows:", train.count())
print("Test rows:", test.count())

# Add SparkML/XGBoost training here.
# Save the trained model under:
# s3a://traffic-models/

spark.stop()
