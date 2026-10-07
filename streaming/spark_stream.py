from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("traffic-stream")
    .getOrCreate()
)

events = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "traffic")
    .option("startingOffsets", "latest")
    .load()
)

# Parse the Kafka JSON payload and write the processed
# streaming results to PostgreSQL here.

query = (
    events.writeStream
    .format("console")
    .outputMode("append")
    .start()
)

query.awaitTermination()
