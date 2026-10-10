from datetime import datetime
from airflow.sdk import DAG, Param
from airflow.providers.standard.operators.bash import BashOperator

D = "/opt/airflow/bin/docker"

def ctr(service):   # find a container by its compose service name
    return f"$({D} ps -qf label=com.docker.compose.service={service})"

SM = f"{D} exec {ctr('spark-master')}"
S3_OPTS = "--packages org.apache.hadoop:hadoop-aws:3.4.2 --conf spark.jars.ivy=/tmp/.ivy2"
STREAM_PKGS = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3,org.postgresql:postgresql:42.7.4"

def sh(task_id, cmd, **kw):
    return BashOperator(task_id=task_id, bash_command=cmd, **kw)

with DAG(
    dag_id="traffic_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,                  # manual; use "@monthly" to retrain monthly
    catchup=False,
    params={"speed": Param(1000, type="integer", description="producer replay speed")},
    default_args={"retries": 1},
    tags=["traffic"],
) as dag:

    check_data = sh("check_data", SM + r""" python3 -c "
import boto3
s3 = boto3.client('s3', endpoint_url='http://seaweedfs:8333', aws_access_key_id='any', aws_secret_access_key='any', region_name='us-east-1')
s3.head_object(Bucket='traffic', Key='raw/yellow_tripdata_2025-01.parquet')
s3.head_object(Bucket='traffic', Key='stream/yellow_tripdata_2025-12.parquet')
print('data ok')" """)

    create_tables = sh("create_tables",
        f"{D} exec -i {ctr('postgres')} psql -U postgres -d traffic < /opt/project/streaming/create_tables.sql")

    clean = sh("clean_taxi",
        f'{SM} /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 4g {S3_OPTS} /opt/project/spark_jobs/clean_taxi.py')

    train = sh("train_models",
        f'{SM} /opt/spark/bin/spark-submit --master "local[2]" --driver-memory 3g {S3_OPTS} /opt/project/ml_pipeline/train_xgb.py')

    reset_stream = sh("reset_stream", f"""
        {D} exec {ctr('postgres')} psql -U postgres -d traffic -c "TRUNCATE scored_trips, zone_hour_metrics;"
        {SM} rm -rf /tmp/checkpoints/taxi_stream
        {D} exec {ctr('kafka')} /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --delete --topic taxi-trips || true
    """)

    start_stream = sh("start_stream", f"""
        {D} exec -d {ctr('spark-master')} bash -c 'echo $$ > /tmp/stream.pid; exec /opt/spark/bin/spark-submit --master "local[2]" --driver-memory 2g --packages {STREAM_PKGS} --conf spark.jars.ivy=/tmp/.ivy2 /opt/project/streaming/spark_stream.py > /tmp/stream.log 2>&1'
        sleep 60
    """)

    producer = sh("run_producer",
        SM + " python3 /opt/project/streaming/producer.py --speed {{ params.speed }}")

    stop_stream = sh("stop_stream",
        f"{SM} bash -c 'sleep 30; kill $(cat /tmp/stream.pid)'",
        trigger_rule="all_done")    # stops the stream even if the producer failed

    check_data >> create_tables >> clean >> train >> reset_stream >> start_stream >> producer >> stop_stream