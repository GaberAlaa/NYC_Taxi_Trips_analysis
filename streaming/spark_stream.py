"""
Spark Structured Streaming: Kafka -> clean -> add category labels -> PostgreSQL (table trips_live)
Live data visualization only, no ML.

Run inside the spark-master container:
  /opt/spark/bin/spark-submit --master "local[2]" --driver-memory 2g \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3,org.postgresql:postgresql:42.7.4 \
    --conf spark.jars.ivy=/tmp/.ivy2 /opt/project/streaming/spark_stream.py
"""
import os

from pyspark.sql import SparkSession, functions as F, types as T

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "taxi-trips")
PG_URL = os.environ.get("PG_URL", "jdbc:postgresql://postgres:5432/traffic")
PG_PROPS = {"user": os.environ.get("PG_USER", "postgres"),
            "password": os.environ.get("PG_PASSWORD", "postgres"),
            "driver": "org.postgresql.Driver"}
CHECKPOINT = os.environ.get("CHECKPOINT", "/tmp/checkpoints/taxi_stream")
TRIGGER = "5 seconds"
MAX_PER_TRIGGER = 20000

D = T.DoubleType()
SCHEMA = T.StructType([
    T.StructField("VendorID", D),
    T.StructField("tpep_pickup_datetime", T.StringType()),
    T.StructField("tpep_dropoff_datetime", T.StringType()),
    T.StructField("passenger_count", D),
    T.StructField("trip_distance", D),
    T.StructField("RatecodeID", D),
    T.StructField("store_and_fwd_flag", T.StringType()),
    T.StructField("PULocationID", D),
    T.StructField("DOLocationID", D),
    T.StructField("payment_type", D),
    T.StructField("fare_amount", D),
    T.StructField("extra", D),
    T.StructField("mta_tax", D),
    T.StructField("tip_amount", D),
    T.StructField("tolls_amount", D),
    T.StructField("improvement_surcharge", D),
    T.StructField("total_amount", D),
    T.StructField("congestion_surcharge", D),
    T.StructField("airport_fee", D),
])


def parse_events(raw):
    return (raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e"))
               .select("e.*"))


def label(col, mapping, default):
    """code -> text label, e.g. 1 -> 'Credit card'."""
    expr = None
    for code, text in mapping.items():
        cond = F.col(col) == code
        expr = F.when(cond, text) if expr is None else expr.when(cond, text)
    return expr.otherwise(default)


def clean_events(events):
    """Same cleaning rules as clean_taxi.py (December 2025), then category labels."""
    df = (events
        .withColumn("pickup_ts", F.to_timestamp("tpep_pickup_datetime"))
        .withColumn("dropoff_ts", F.to_timestamp("tpep_dropoff_datetime"))
        .dropna(subset=["pickup_ts", "dropoff_ts", "passenger_count", "trip_distance",
                        "fare_amount", "PULocationID", "DOLocationID"])
        .withColumn("duration_minutes",
                    (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 60)
        .withColumn("speed_mph", F.col("trip_distance") / (F.col("duration_minutes") / 60))
        .filter(
            (F.col("pickup_ts") >= "2025-12-01") & (F.col("pickup_ts") < "2026-01-01") &
            F.col("passenger_count").between(1, 6) &
            F.col("trip_distance").between(0.1, 100) &
            F.col("fare_amount").between(2.5, 500) &
            (F.col("total_amount") > 0) &
            F.col("duration_minutes").between(1, 180) &
            (F.col("speed_mph") < 80) &
            F.col("PULocationID").between(1, 265) &
            F.col("DOLocationID").between(1, 265)))

    zero = lambda c: F.coalesce(F.col(c), F.lit(0.0)).alias(c)
    df = df.select(
        "pickup_ts", "dropoff_ts",
        F.coalesce(F.col("VendorID").cast("int"), F.lit(0)).alias("vendor_id"),
        F.col("passenger_count").cast("int").alias("passenger_count"),
        "trip_distance",
        F.coalesce(F.col("RatecodeID").cast("int"), F.lit(99)).alias("ratecode_id"),
        F.coalesce("store_and_fwd_flag", F.lit("N")).alias("store_and_fwd_flag"),
        F.col("PULocationID").cast("int").alias("pu_zone"),
        F.col("DOLocationID").cast("int").alias("do_zone"),
        F.coalesce(F.col("payment_type").cast("int"), F.lit(5)).alias("payment_type"),
        "fare_amount", zero("extra"), zero("mta_tax"), zero("tip_amount"), zero("tolls_amount"),
        zero("improvement_surcharge"), "total_amount", zero("congestion_surcharge"),
        zero("airport_fee"), "duration_minutes", "speed_mph")

    hour = F.hour("pickup_ts")
    iso_dow = ((F.dayofweek("pickup_ts") + 5) % 7) + 1          # Mon=1 ... Sun=7
    return (df
        .withColumn("pickup_hour", hour)
        .withColumn("weekday", F.concat(iso_dow.cast("string"), F.lit(" "),
                                        F.date_format("pickup_ts", "E")))
        .withColumn("day_type", F.when(iso_dow >= 6, "Weekend").otherwise("Weekday"))
        .withColumn("time_of_day",
                    F.when(hour < 6, "1 Night (0-5h)").when(hour < 12, "2 Morning (6-11h)")
                     .when(hour < 18, "3 Afternoon (12-17h)").otherwise("4 Evening (18-23h)"))
        .withColumn("vendor_name", label("vendor_id", {1: "Creative Mobile", 2: "VeriFone"}, "Other"))
        .withColumn("rate_name", label("ratecode_id", {1: "Standard", 2: "JFK", 3: "Newark",
                    4: "Nassau/Westchester", 5: "Negotiated", 6: "Group ride"}, "Other"))
        .withColumn("payment_name", label("payment_type", {0: "Flex fare", 1: "Credit card",
                    2: "Cash", 3: "No charge", 4: "Dispute", 5: "Unknown", 6: "Voided"}, "Other"))
        .withColumn("tip_status",
                    F.when(F.col("payment_type") == 1,
                           F.when(F.col("tip_amount") > 0, "Tipped").otherwise("No tip"))
                     .otherwise("Not recorded (non-card)"))
        .withColumn("trip_type",
                    F.when(F.col("ratecode_id").isin(2, 3) | (F.col("airport_fee") > 0), "Airport")
                     .otherwise("City"))
        .withColumn("distance_bucket",
                    F.when(F.col("trip_distance") < 1, "1) under 1 mi")
                     .when(F.col("trip_distance") < 2, "2) 1-2 mi")
                     .when(F.col("trip_distance") < 5, "3) 2-5 mi")
                     .when(F.col("trip_distance") < 10, "4) 5-10 mi").otherwise("5) over 10 mi"))
        .withColumn("group_size",
                    F.when(F.col("passenger_count") == 1, "1 passenger")
                     .when(F.col("passenger_count") == 2, "2 passengers")
                     .when(F.col("passenger_count") <= 4, "3-4 passengers").otherwise("5-6 passengers"))
        .withColumn("traffic_level",
                    F.when(F.col("speed_mph") < 8, "1 Heavy (<8 mph)")
                     .when(F.col("speed_mph") < 15, "2 Moderate (8-15 mph)")
                     .otherwise("3 Free-flowing (15+ mph)")))


def process_batch(batch_df, batch_id):
    batch_df = batch_df.persist()
    n = batch_df.count()
    if n == 0:
        batch_df.unpersist()
        return
    out = batch_df.withColumn("batch_id", F.lit(batch_id).cast("long"))
    out.write.jdbc(PG_URL, "trips_live", mode="append", properties=PG_PROPS)
    last = batch_df.agg(F.max("dropoff_ts"), F.avg("speed_mph")).first()
    print(f"batch {batch_id}: {n:,} trips | simulated time {last[0]} | "
          f"avg speed {last[1]:.1f} mph", flush=True)
    batch_df.unpersist()


def main():
    spark = (SparkSession.builder.appName("taxi_stream")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger", MAX_PER_TRIGGER)
           .option("failOnDataLoss", "false")
           .load())

    query = (clean_events(parse_events(raw))
             .writeStream.foreachBatch(process_batch)
             .option("checkpointLocation", CHECKPOINT)
             .trigger(processingTime=TRIGGER)
             .start())
    print(f"streaming from Kafka topic '{TOPIC}' -> Postgres trips_live; waiting for data...", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()