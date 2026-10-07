import os
import boto3

def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://seaweedfs:8333"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY", "any"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY", "any"),
        region_name=os.getenv("S3_REGION", "us-east-1"),
    )
