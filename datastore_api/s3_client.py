import logging

import boto3
from botocore.exceptions import ClientError

from datastore_api.config import get_settings

LOGGER = logging.getLogger(__name__)


class S3Client:
    """Wrapper for S3 functionality."""

    def __init__(self) -> None:
        """ """
        self.settings = get_settings().s3
        self.resource = boto3.resource(
            "s3",
            endpoint_url=self.settings.endpoint,
            aws_access_key_id=self.settings.access_key,
            aws_secret_access_key=self.settings.secret_key,
        )
        self.client = boto3.client(
            "s3",
            endpoint_url=self.settings.endpoint,
            aws_access_key_id=self.settings.access_key,
            aws_secret_access_key=self.settings.secret_key,
        )
        self.simulated_data_bucket = self.resource.Bucket(
            self.settings.simulated_data_bucket,
        )
        self.bucket = self.resource.Bucket(self.settings.storage_bucket)

    def create_presigned_url(self, object_name, expiration=3600):
        """_summary_

        Args:
            object_name (_type_): _description_
            expiration (int, optional): _description_. Defaults to 3600.
        """
        try:
            response = self.client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket.name, "Key": object_name},
                ExpiresIn=expiration,
            )
        except ClientError as e:
            LOGGER.error(e)
            return None

        return response
