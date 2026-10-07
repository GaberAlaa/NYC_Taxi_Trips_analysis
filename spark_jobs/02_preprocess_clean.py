from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("traffic-preprocess").getOrCreate()

df = spark.read.parquet("s3a://traffic-raw/")

# Add project-specific cleaning and transformations here.
clean = df.dropna()

clean.write.mode("overwrite").parquet("s3a://traffic-clean/")

spark.stop()
