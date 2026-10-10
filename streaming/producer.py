#!/usr/bin/env python3
"""
Replay the held-out Dec 2025 NYC taxi trips into Kafka as if they were arriving live.

* Reads s3://traffic/stream/yellow_tripdata_2025-12.parquet from SeaweedFS
* Sorts trips by dropoff time (a trip record "arrives" when the trip ends)
* Sends one JSON message per trip to the topic, keyed by pickup zone
* --speed 300 means 300 simulated seconds pass per real second
  (1 simulated hour = 12 s, 1 simulated day ~ 5 min)

Run inside the spark-master container (the broker only advertises kafka:9092):
  python3 /opt/project/streaming/producer.py --speed 300
"""
import argparse
import os
import time

import boto3
import pandas as pd
from kafka import KafkaProducer
import pyarrow.parquet as pq

COLUMNS = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
           "trip_distance", "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID",
           "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
           "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee"]
TIME_COLS = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    p.add_argument("--topic", default="taxi-trips")
    p.add_argument("--speed", type=float, default=300.0, help="simulated seconds per real second")
    p.add_argument("--limit", type=int, default=0, help="only send the first N trips (0 = all)")
    p.add_argument("--start", default="2025-12-01", help="first dropoff time to replay")
    p.add_argument("--end", default="2026-01-01", help="replay dropoffs before this time")
    p.add_argument("--chunk", type=int, default=200, help="trips sent per pacing step")
    p.add_argument("--s3-endpoint", default=os.environ.get("S3_ENDPOINT", "http://seaweedfs:8333"))
    p.add_argument("--bucket", default="traffic")
    p.add_argument("--s3-key", default="stream/yellow_tripdata_2025-12.parquet")
    return p.parse_args(argv)


def load_trips(args):
    local = "/tmp/" + os.path.basename(args.s3_key)
    if not os.path.exists(local):
        print(f"downloading s3://{args.bucket}/{args.s3_key} ...", flush=True)
        s3 = boto3.client("s3", endpoint_url=args.s3_endpoint, aws_access_key_id="any",
                          aws_secret_access_key="any", region_name="us-east-1")
        s3.download_file(args.bucket, args.s3_key, local)

    lower = {n.lower(): n for n in pq.ParquetFile(local).schema.names}   # e.g. Airport_fee vs airport_fee
    found = {c: lower[c.lower()] for c in COLUMNS if c.lower() in lower}
    df = pd.read_parquet(local, columns=list(found.values())).rename(columns={v: k for k, v in found.items()})
    df = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID"])
    # keep only the replay window; stray timestamps would create huge gaps in the simulated clock.
    # Other dirt (bad distances, fares, ...) is left in on purpose: Spark cleans it.
    drop = df["tpep_dropoff_datetime"]
    df = df[(drop >= pd.Timestamp(args.start)) & (drop < pd.Timestamp(args.end))]
    df = df.sort_values("tpep_dropoff_datetime", kind="stable").reset_index(drop=True)
    if args.limit:
        df = df.head(args.limit)
    return df


def main(argv=None):
    args = parse_args(argv)
    df = load_trips(args)
    n = len(df)
    if n == 0:
        raise SystemExit("no trips to replay in that time window")
    print(f"replaying {n:,} trips from {df.tpep_dropoff_datetime.iloc[0]} to "
          f"{df.tpep_dropoff_datetime.iloc[-1]} at {args.speed:g}x -> topic '{args.topic}'", flush=True)

    producer = KafkaProducer(bootstrap_servers=args.bootstrap, linger_ms=20,
                             batch_size=64 * 1024, acks=1)
    ts = df["tpep_dropoff_datetime"]
    offsets = (ts - ts.iloc[0]).dt.total_seconds().to_numpy()   # simulated seconds since first trip
    keys = df["PULocationID"].astype("int64").astype(str).to_numpy()

    t0 = time.time()
    last_log, sent = t0, 0
    for i in range(0, n, args.chunk):
        wait = t0 + offsets[i] / args.speed - time.time()
        if wait > 0:
            time.sleep(wait)
        chunk = df.iloc[i:i + args.chunk].copy()
        for c in TIME_COLS:
            chunk[c] = chunk[c].dt.strftime("%Y-%m-%d %H:%M:%S")
        lines = chunk.to_json(orient="records", lines=True).splitlines()
        for key, line in zip(keys[i:i + args.chunk], lines):
            producer.send(args.topic, key=key.encode(), value=line.encode())
        sent += len(lines)
        now = time.time()
        if now - last_log >= 5:
            print(f"simulated time {ts.iloc[min(i + args.chunk, n) - 1]} | sent {sent:,}/{n:,} | "
                  f"{sent / (now - t0):,.0f} msg/s", flush=True)
            last_log = now
    producer.flush()
    print(f"done: sent {sent:,} trips in {time.time() - t0:,.0f}s", flush=True)


if __name__ == "__main__":
    main()