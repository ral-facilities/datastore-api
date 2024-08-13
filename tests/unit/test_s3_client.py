from pydantic import UUID4

from datastore_api.s3_client import S3Client
from tests.fixtures import (
    bucket_creation,
    bucket_deletion,
    mock_fts3_settings,
    s3_client,
    s3_settings,
    submit,
    tag_bucket,
)


class TestS3Client:
    def test_create_bucket(self, s3_client: S3Client, bucket_deletion: None):
        response = s3_client.create_bucket()
        bucket_name = response["Location"][1:]
        assert bucket_name
        UUID4(bucket_name)
        assert bucket_name in s3_client.list_buckets()

        # Test for bucket with existing name
        response = s3_client.create_bucket(bucket_name="miniotestbucket")
        bucket_name = response["Location"][1:]
        assert bucket_name != "miniotestbucket"
        UUID4(bucket_name)
        assert bucket_name in s3_client.list_buckets()

    def test_delete_bucket(self, s3_client: S3Client, bucket_creation: str):
        s3_client.delete_bucket(bucket_name=bucket_creation)
        assert bucket_creation not in s3_client.list_buckets()

    def test_create_presigned_url(self, s3_client: S3Client):
        url = s3_client.create_presigned_url("test", "miniotestbucket")
        assert url.startswith("http://127.0.0.1:9000/miniotestbucket/test")

    def test_list_bucket_objects(self, s3_client: S3Client):
        object_names = s3_client.list_bucket_objects("miniotestbucket", max_keys=2)
        assert "test" in object_names
        assert "test2" in object_names
        assert "test3" in object_names

    def test_list_buckets(self, s3_client: S3Client, bucket_deletion: None):
        bucket = s3_client.create_bucket()
        bucket_names = s3_client.list_buckets()
        assert len(bucket_names) == 2
        assert bucket["Location"][1:] in bucket_names
        assert "miniotestbucket" in bucket_names

    def test_tag_bucket(self, s3_client: S3Client):
        tags = [{"Key": "Environment", "Value": "Test"}]
        response = s3_client.get_bucket_tags(bucket_name="miniotestbucket")
        assert tags != response
        s3_client.tag_bucket(bucket_name="miniotestbucket", tags=tags)
        response = s3_client.get_bucket_tags(bucket_name="miniotestbucket")
        assert tags == response

    def test_get_bucket_tags(self, s3_client: S3Client, tag_bucket: None):
        tag_list = s3_client.get_bucket_tags(bucket_name="miniotestbucket")
        assert {
            "Key": "00000000-0000-0000-0000-000000000000",
            "Value": "STAGING",
        } in tag_list
