from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("traffic-ingest").getOrCreate()

df = spark.read.option("header", True).csv("/opt/project/data/raw")

df.write.mode("overwrite").parquet("s3a://traffic-raw/")

spark.stop()
