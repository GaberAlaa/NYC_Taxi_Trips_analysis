from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="traffic_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["traffic"],
) as dag:

    preprocess = BashOperator(
        task_id="preprocess",
        bash_command=(
            "echo 'Run: spark-submit "
            "/opt/project/spark_jobs/02_preprocess_clean.py'"
        ),
    )

    train = BashOperator(
        task_id="train",
        bash_command=(
            "echo 'Run: spark-submit "
            "/opt/project/ml_pipeline/train.py'"
        ),
    )

    preprocess >> train
