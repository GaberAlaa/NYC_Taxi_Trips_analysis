from functools import reduce
from pyspark.sql import SparkSession, functions as F

MONTHS   = [f"2025-{m:02d}" for m in range(1, 12)]   # Jan..Nov (Dec = streaming)
RAW      = "s3a://traffic/raw/yellow_tripdata_{}.parquet"
S3       = "s3a://traffic/clean"
LOCAL    = "file:///opt/project/data/clean"

spark = (SparkSession.builder.appName("clean_taxi_2025")
    .config("spark.hadoop.fs.s3a.endpoint", "http://seaweedfs:8333")
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.access.key", "any")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.secret.key", "any")
    .config("spark.sql.shuffle.partitions", "64")
    .getOrCreate())

def load(month):
    """Read one month and force a consistent schema (TLC types drift between months)."""
    d = spark.read.parquet(RAW.format(month))
    return d.select(
        F.col("VendorID").cast("int").alias("VendorID"),
        F.col("tpep_pickup_datetime").cast("timestamp").alias("pickup_ts"),
        F.col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_ts"),
        F.col("passenger_count").cast("int").alias("passenger_count"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("RatecodeID").cast("int").alias("RatecodeID"),
        F.col("PULocationID").cast("int").alias("PULocationID"),
        F.col("DOLocationID").cast("int").alias("DOLocationID"),
        F.col("payment_type").cast("int").alias("payment_type"),
        F.col("fare_amount").cast("double").alias("fare_amount"),
        F.col("tip_amount").cast("double").alias("tip_amount"),
        F.col("tolls_amount").cast("double").alias("tolls_amount"),
        F.col("total_amount").cast("double").alias("total_amount"),
    )

raw = reduce(lambda a, b: a.unionByName(b), [load(m) for m in MONTHS])
before = raw.count()

df = (raw
    .dropna(subset=["pickup_ts", "dropoff_ts", "passenger_count", "trip_distance",
                    "fare_amount", "PULocationID", "DOLocationID"])
    .withColumn("trip_minutes",
        (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 60)
    .withColumn("speed_mph", F.col("trip_distance") / (F.col("trip_minutes") / 60))
    .filter(
        (F.col("pickup_ts") >= "2025-01-01") & (F.col("pickup_ts") < "2025-12-01") &
        F.col("passenger_count").between(1, 6) &
        F.col("trip_distance").between(0.1, 100) &
        F.col("fare_amount").between(2.5, 500) &
        (F.col("total_amount") > 0) &
        F.col("trip_minutes").between(1, 180) &
        (F.col("speed_mph") < 80) &
        F.col("PULocationID").between(1, 265) &
        F.col("DOLocationID").between(1, 265))
    .withColumn("pickup_date",  F.to_date("pickup_ts"))
    .withColumn("pickup_month", F.month("pickup_ts"))
    .withColumn("pickup_hour",  F.hour("pickup_ts"))
    .withColumn("pickup_dow",   F.dayofweek("pickup_ts"))   # 1=Sun ... 7=Sat
    .withColumn("is_weekend",   F.col("pickup_dow").isin(1, 7).cast("int"))
)

# 1) full clean data -> SeaweedFS, then re-read it and copy to your machine
df.write.mode("overwrite").partitionBy("pickup_month").parquet(f"{S3}/yellow_2025")
clean = spark.read.parquet(f"{S3}/yellow_2025")
after = clean.count()
print(f"ROWS BEFORE: {before}  AFTER: {after}  DROPPED: {before - after}")
clean.write.mode("overwrite").partitionBy("pickup_month").parquet(f"{LOCAL}/yellow_2025")

# 2) small tables for the dashboard -> both places
def save(frame, name):
    frame.coalesce(1).write.mode("overwrite").parquet(f"{S3}/{name}")
    frame.coalesce(1).write.mode("overwrite").parquet(f"{LOCAL}/{name}")

save(clean.groupBy("pickup_date", "pickup_month", "pickup_dow", "pickup_hour").agg(
        F.count("*").alias("trips"),
        F.sum("trip_distance").alias("sum_distance"),
        F.sum("fare_amount").alias("sum_fare"),
        F.sum("trip_minutes").alias("sum_minutes")), "agg_hourly")

save(clean.groupBy("PULocationID", "pickup_month", "pickup_dow", "pickup_hour")
          .agg(F.count("*").alias("trips")), "agg_zone")

save(clean.sample(fraction=0.0025, seed=42)
          .select("trip_distance", "fare_amount", "trip_minutes", "speed_mph", "pickup_hour"),
     "sample")

print("DONE")
spark.stop()