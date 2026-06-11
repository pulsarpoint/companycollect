"""One-time idempotent bucket creation for Finland source buckets."""

import os

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

BUCKETS = ["source-finland-prhytj", "source-finland-prh-xbrl"]


def main() -> None:
    client = boto3.client(
        "s3",
        endpoint_url=os.environ["CORPSCOUT_S3_ENDPOINT"],
        aws_access_key_id=os.environ["CORPSCOUT_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["CORPSCOUT_S3_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )
    for bucket in BUCKETS:
        try:
            client.create_bucket(Bucket=bucket)
            print(f"created {bucket}")
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                print(f"exists {bucket}")
            else:
                raise


if __name__ == "__main__":
    main()
