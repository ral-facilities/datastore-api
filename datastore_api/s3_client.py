import logging

import boto3

# from botocore.exceptions import ClientError

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

    # TODO: expiration? / what region should be default?
    def create_bucket(self, bucket_name: str) -> dict:
        """Creates a new s3 storage bucket
        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-example-creating-buckets.html
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/create_bucket.html
        \f

        Args:
            bucket_name (str): Name for the bucket

        Returns:
            dict: a dictionary with 'Location', which is the forward slash
                followed by the name of the bucket
        """
        return self.client.create_bucket(Bucket=bucket_name)

    # TODO: do we need ExpectedBucketOwner?
    def delete_bucket(self, bucket_name):
        """Deletes a specified bucket and its contents
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/delete_bucket.html

        Args:
            bucket_name (_type_): _description_
        """
        # First need to empty the bucket
        # https://stackoverflow.com/questions/43326493/what-is-the-fastest-way-to-empty-s3-bucket-using-boto3
        bucket = self.resource.Bucket(bucket_name)
        bucket.objects.all().delete()

        self.client.delete_bucket(Bucket=bucket_name)

    def create_presigned_url(self, object_name, bucket_name, expiration=3600):
        """Creates the download link for a single file in a bucket
        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html

        Args:
            object_name (_type_): _description_
            expiration (int, optional): _description_. Defaults to 3600.
        """
        # This try/except block is included in this aws example:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html
        # Now commented out to avoid supressing errors
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
