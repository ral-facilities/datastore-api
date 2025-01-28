from botocore.exceptions import ClientError
from fastapi import HTTPException
import pytest
from pytest_mock import mocker, MockerFixture

from datastore_api.config import Settings
from datastore_api.controllers.bucket_controller import BucketController
from datastore_api.models.job import JobState
from datastore_api.models.transfer import BucketAcl
from tests.fixtures import (
    bucket_name_incomplete,
    bucket_name_private,
    cache_bucket,
    mock_fts3_settings,
    STATUSES,
    submit,
)


class TestBucketController:
    def test_init_failure(self, mock_fts3_settings: Settings):
        with pytest.raises(HTTPException) as e:
            BucketController(storage_key="echo", name="cache-bucket")
        assert e.exconly() == (
            "fastapi.exceptions.HTTPException: 403: "
            "Access to global S3 cache is forbidden"
        )

    def test_complete(self, bucket_name_incomplete: str, mocker: MockerFixture):
        module = "datastore_api.clients.fts3_client.fts3.get_jobs_statuses"
        fts_status_mock = mocker.patch(module)
        fts_status_mock.return_value = {
            "job_id": "00000000-0000-0000-0000-000000000000",
            "job_state": "ACTIVE",
            "files": [],
        }

        bucket_controller = BucketController(
            storage_key="echo",
            name=bucket_name_incomplete,
        )
        assert not bucket_controller.complete

    def test_bucket_controller_update_job_ids(
        self,
        mock_fts3_settings: Settings,
        cache_bucket: str,
        bucket_name_incomplete: str,
    ):
        bucket_controller = BucketController(
            storage_key="echo",
            name=bucket_name_incomplete,
        )
        bucket_controller._acl = BucketAcl.PUBLIC_READ
        objects = list(bucket_controller.bucket.objects.all())

        assert len(objects) == 1
        assert objects[0].key == ".job_ids"

        state_counter = bucket_controller.update_job_ids(
            statuses=STATUSES,
            check_files=False,
        )
        objects = list(bucket_controller.bucket.objects.all())

        assert state_counter.state == JobState.finished_dirty
        assert len(objects) == 2
        assert objects[0].key == ".job_ids"
        assert objects[1].key == "test0"

    def test_get_data_private(
        self,
        mock_fts3_settings: Settings,
        bucket_name_private: str,
        mocker: MockerFixture,
    ):
        echo_url = mock_fts3_settings.fts3.storage_endpoints["echo"].url
        expected = f"{echo_url}{bucket_name_private}/test?"
        bucket_controller = BucketController(
            storage_key="echo",
            name=bucket_name_private,
        )
        data_dict = bucket_controller.get_data(expiration=1)
        assert len(data_dict) == 1
        assert "test" in data_dict
        assert data_dict["test"].startswith(expected)

    def test_get_data_public(
        self,
        mock_fts3_settings: Settings,
        bucket_name_private: str,
        mocker: MockerFixture,
    ):
        """Due to differences in how Ceph (the planned production environment) and minio
        (containerised dev/test S3) implement ACLs and permissions, the method that
        works for Ceph does not work for minio, so mock this to ensure the parser is
        correct regardless of what S3 backend is used.
        """
        bucket_controller = BucketController(
            storage_key="echo",
            name=bucket_name_private,
        )
        mock_get_bucket_acl = mocker.MagicMock()
        uri = "http://acs.amazonaws.com/groups/global/AllUsers"
        grant = {"Grantee": {"Type": "Group", "URI": uri}, "Permission": "READ"}
        mock_get_bucket_acl.return_value = {"Grants": [grant]}
        bucket_controller.s3_client.client.get_bucket_acl = mock_get_bucket_acl

        data_dict = bucket_controller.get_data(expiration=1)
        echo_url = mock_fts3_settings.fts3.storage_endpoints["echo"].url
        expected = {"bucket": f"{echo_url}/{bucket_name_private}"}
        assert data_dict == expected

    def test_get_data_failure(self, bucket_name_incomplete: str, mocker: MockerFixture):
        module = "datastore_api.clients.fts3_client.fts3.get_jobs_statuses"
        fts_status_mock = mocker.patch(module)
        fts_status_mock.return_value = {
            "job_id": "00000000-0000-0000-0000-000000000000",
            "job_state": "ACTIVE",
            "files": [],
        }

        bucket_controller = BucketController(
            storage_key="echo",
            name=bucket_name_incomplete,
        )
        with pytest.raises(HTTPException) as e:
            bucket_controller.get_data(expiration=1)
        assert e.exconly() == (
            "fastapi.exceptions.HTTPException: 400: "
            "Restoration of requested data still ongoing"
        )

    def test_delete_no_raise(self, bucket_name_private: str):
        bucket_controller = BucketController(
            storage_key="echo",
            name=bucket_name_private,
        )
        bucket_controller.delete()
        bucket_controller.delete()
        # No assert, as if we delete twice without exception it's behaving as intended

    def test_delete_re_raise(self, bucket_name_private: str, mocker: MockerFixture):
        bucket_controller = BucketController(
            storage_key="echo",
            name=bucket_name_private,
        )
        mock_delete = mocker.MagicMock()
        mock_delete.side_effect = ClientError({}, "delete")
        bucket_controller.bucket.delete = mock_delete
        with pytest.raises(ClientError) as e:
            bucket_controller.delete()

        assert e.exconly() == (
            "botocore.exceptions.ClientError: "
            "An error occurred (Unknown) when calling the delete operation: Unknown"
        )
