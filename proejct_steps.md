<!-- ### first step loading the data into seaweed container

```
docker compose exec spark-master python3 -c "import boto3; boto3.client('s3', endpoint_url='http://seaweedfs:8333', aws_access_key_id='any', aws_secret_access_key='any', region_name='us-east-1').create_bucket(Bucket='traffic')"
```

```
1..11 | ForEach-Object {
$f = "yellow_tripdata_2025-{0:D2}.parquet" -f $_
  curl.exe -F "file=@data/raw/$f" "http://localhost:8888/buckets/traffic/raw/$f"
}
```

check

```
http://localhost:8888/buckets/traffic/raw/
```

you should see the data on seaweed server

```
docker compose exec spark-master bash -c "ls /opt/spark/jars | grep -E 'hadoop-client|hadoop-aws'"
```

### run clean_taxi.py

```
docker compose exec spark-master /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 4g --packages org.apache.hadoop:hadoop-aws:3.4.2 --conf spark.jars.ivy=/tmp/.ivy2 /opt/project/spark_jobs/clean_taxi.py
```

### run training models

```
docker compose exec spark-master /opt/spark/bin/spark-submit --master "local[2]" --driver-memory 3g --packages org.apache.hadoop:hadoop-aws:3.4.2 --conf spark.jars.ivy=/tmp/.ivy2 /opt/project/ml_pipeline/train_xgb.py
``` -->

### create the postgree tables

```
Get-Content streaming\create_tables.sql | docker compose exec -T postgres psql -U postgres -d traffic
```

### create topic

```
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic taxi-trips --partitions 1 --replication-factor 1
```

### start spark streaming

```
docker compose exec spark-master /opt/spark/bin/spark-submit --master "local[2]" --driver-memory 2g --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3,org.postgresql:postgresql:42.7.4 --conf spark.jars.ivy=/tmp/.ivy2 /opt/project/streaming/spark_stream.py
```

### start producer

```
docker compose exec spark-master python3 /opt/project/streaming/producer.py --speed 30000
```

use the following for a quick demo

```
docker compose exec spark-master python3 /opt/project/streaming/producer.py --speed 3000 --limit 10000
```

### Clear cache

```
docker compose exec spark-master rm -rf /tmp/checkpoints/taxi_stream
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic taxi-trips
docker compose exec postgres psql -U postgres -d traffic -c "TRUNCATE TABLE trips_live RESTART IDENTITY CASCADE;"
```

airFlow{"admin": "R2Vg2f8pXz7m5Gue"}
