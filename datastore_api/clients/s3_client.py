from functools import lru_cache
import logging

import boto3
from mypy_boto3_s3 import S3Client as S3ClientBoto3, S3ServiceResource

from datastore_api.config import get_settings

LOGGER = logging.getLogger(__name__)


class S3Client:
    """Wrapper for S3 functionality."""

    def __init__(self, key: str) -> None:
        """Initialise the client with the cached `s3_settings`."""
        settings = get_settings()
        storage_endpoint = settings.fts3.storage_endpoints[key]
        self.endpoint = storage_endpoint.url
        self.cache_bucket = storage_endpoint.cache_bucket
        self.resource: S3ServiceResource = boto3.resource(
            "s3",
            endpoint_url=storage_endpoint.url,
            aws_access_key_id=storage_endpoint.access_key.get_secret_value(),
            aws_secret_access_key=storage_endpoint.secret_key.get_secret_value(),
        )
        self.client: S3ClientBoto3 = boto3.client(
            "s3",
            endpoint_url=storage_endpoint.url,
            aws_access_key_id=storage_endpoint.access_key.get_secret_value(),
            aws_secret_access_key=storage_endpoint.secret_key.get_secret_value(),
        )

    def create_presigned_url(self, object_name: str, bucket_name: str, expiration=3600):
        """Creates the download link for a single file in a bucket
        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html

        Args:
            object_name (str): Name of the object in S3 bucket.
            bucket_name (str): Name of the bucket containing the requested object.
            expiration (int, optional): Expiration date of the download url in seconds.
                Defaults to 3600.

        Returns:
            str: Presigned URL as a string
        """
        # This try/except block is included in this aws example:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html
        # Now commented out to avoid suppressing errors
        # try:
        response = self.client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket_name, "Key": object_name},
            ExpiresIn=expiration,
        )
        # except ClientError as e:
        #     LOGGER.error(e)
        #     return None

        return response

    def list_buckets(self) -> list[str]:
        """Lists all owned buckets

        Returns:
            list[str]: A list of bucket names
        """
        bucket_names = []
        for bucket in self.client.list_buckets()["Buckets"]:
            bucket_names.append(bucket["Name"])
        return bucket_names


@lru_cache
def get_s3_client(key: str) -> S3Client:
    """Initialise and cache the client for making calls to S3.

    Returns:
        S3Client: Wrapper for calls to S3.
    """
    return S3Client(key=key)
