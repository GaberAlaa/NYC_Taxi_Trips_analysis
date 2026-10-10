### first step loading the data into seaweed container

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
```
