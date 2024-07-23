import logging
from uuid import uuid4

import boto3

from datastore_api.config import get_settings

LOGGER = logging.getLogger(__name__)


class S3Client:
    """Wrapper for S3 functionality."""

    def __init__(self) -> None:
        """Initialise the client with the cached `s3_settings`."""
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
    def create_bucket(self) -> dict:
        """Creates a new s3 storage bucket
        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-example-creating-buckets.html
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/create_bucket.html

        Args:
            bucket_name (str): Name for the bucket

        Returns:
            dict: a dictionary with 'Location', which is the forward slash
                followed by the name of the bucket
        """
        bucket_name = str(uuid4())
        try:
            bucket = self.client.create_bucket(Bucket=bucket_name)
        except self.client.exceptions.BucketAlreadyOwnedByYou:
            bucket = self.create_bucket()
        return bucket

    def delete_bucket(self, bucket_name: str):
        """Deletes a specified bucket and its contents
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/delete_bucket.html

        Args:
            bucket_name (str): Name of the bucket to be deleted
        """
        # First need to empty the bucket
        # https://stackoverflow.com/questions/43326493/what-is-the-fastest-way-to-empty-s3-bucket-using-boto3
        bucket = self.resource.Bucket(bucket_name)
        bucket.objects.all().delete()

        self.client.delete_bucket(Bucket=bucket_name)

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

    def list_bucket_objects(self, bucket_name: str) -> list[str]:
        """Lists objects in a S3 bucket

        Args:
            bucket_name (str): Name of the bucket

        Returns:
            list[str]: List of object names
        """
        object_names = []
        # TODO: list_objects returns max 1000 objects. is it enough?
        # Also, v2 is available:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_objects_v2.html
        response = self.client.list_objects(Bucket=bucket_name)

        for obj in response["Contents"]:
            object_names.append(obj["Key"])

        while response["IsTruncated"]:
            response = self.client.list_objects(
                Bucket=bucket_name,
                Marker=response["NextMarker"],
            )
            for obj in response["Contents"]:
                object_names.append(obj["Key"])

        return object_names

    def list_buckets(self) -> list[str]:
        """Lists all owned buckets

        Returns:
            list[str]: A list of bucket names
        """
        bucket_names = []
        for bucket in self.client.list_buckets()["Buckets"]:
            bucket_names.append(bucket["Name"])
        return bucket_names

    def tag_bucket(self, bucket_name: str, tags: list) -> None:
        """Tag a bucket with tags from a list
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_bucket_tagging.html

        Args:
            bucket_name (str): Name of the bucket to tag.
            tags (list): List of tags in a form of dict with "Key" and "Value" keys.
        """
        self.client.put_bucket_tagging(Bucket=bucket_name, Tagging={"TagSet": tags})

    def get_bucket_tags(self, bucket_name: str) -> list[dict]:
        """Get tags from a specified bucket
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_bucket_tagging.html

        Args:
            bucket_name (str): Name of the bucket

        Returns:
            list[dict]: List of tags in a form of dict with "Key" and "Value" keys.
        """
        response = self.client.get_bucket_tagging(Bucket=bucket_name)
        return response["TagSet"]
