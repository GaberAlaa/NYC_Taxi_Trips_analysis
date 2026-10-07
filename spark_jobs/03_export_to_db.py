from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("traffic-export").getOrCreate()

df = spark.read.parquet("s3a://traffic-clean/")

df.write.format("jdbc")     .option("url", "jdbc:postgresql://postgres:5432/traffic")     .option("dbtable", "traffic_clean")     .option("user", "postgres")     .option("password", "postgres")     .mode("overwrite")     .save()

spark.stop()
