from __future__ import annotations

import os
from functools import lru_cache
from typing import Optional

import boto3
from boto3.session import Session
from dotenv import load_dotenv

load_dotenv()


@lru_cache()
def get_s3_client():
    access_key = os.getenv("S3_ACCESS_KEY")
    secret_key = os.getenv("S3_SECRET_KEY")
    region = os.getenv("S3_REGION") or "us-east-1"
    if not access_key or not secret_key:
        raise RuntimeError("S3 credentials are not configured")

    session = Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )
    return session.client("s3")


def get_s3_bucket() -> str:
    bucket = os.getenv("S3_BUCKET")
    if not bucket:
        raise RuntimeError("S3_BUCKET is not configured")
    return bucket


__all__ = ["get_s3_client", "get_s3_bucket"]
