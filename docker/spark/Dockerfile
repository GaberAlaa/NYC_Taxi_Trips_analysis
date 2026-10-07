FROM apache/spark:4.1.3-python3

USER root

RUN pip install --no-cache-dir     pandas     pyarrow     boto3     kafka-python     xgboost

WORKDIR /opt/project

USER spark
