"""AWS client factory pointing at Floci by default (override with AWS_ENDPOINT_URL)."""
import os


def client(service: str):
    import boto3  # imported lazily so dry-runs and unit tests do not need boto3

    return boto3.client(
        service,
        endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
    )
