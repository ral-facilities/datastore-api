from fastapi import HTTPException
import pytest
from pytest_mock import mocker, MockerFixture

from datastore_api.config import Settings
from datastore_api.controllers.bucket_controller import BucketController
from tests.fixtures import (
    bucket_name_incomplete,
    bucket_name_private,
    mock_fts3_settings,
    submit,
)


class TestBucketController:
    def test_init_failure(self):
        with pytest.raises(HTTPException) as e:
            BucketController(name="cache-bucket")
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

        bucket_controller = BucketController(name=bucket_name_incomplete)
        assert not bucket_controller.complete

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
        bucket_controller = BucketController(name=bucket_name_private)
        mock_get_bucket_acl = mocker.MagicMock()
        uri = "http://acs.amazonaws.com/groups/global/AllUsers"
        grant = {"Grantee": {"Type": "Group", "URI": uri}, "Permission": "READ"}
        mock_get_bucket_acl.return_value = {"Grants": [grant]}
        bucket_controller.s3_client.client.get_bucket_acl = mock_get_bucket_acl

        data_dict = bucket_controller.get_data(expiration=1)
        expected = {"bucket": f"{mock_fts3_settings.s3.endpoint}/{bucket_name_private}"}
        assert data_dict == expected

    def test_get_data_failure(self, bucket_name_incomplete: str, mocker: MockerFixture):
        module = "datastore_api.clients.fts3_client.fts3.get_jobs_statuses"
        fts_status_mock = mocker.patch(module)
        fts_status_mock.return_value = {
            "job_id": "00000000-0000-0000-0000-000000000000",
            "job_state": "ACTIVE",
            "files": [],
        }

        bucket_controller = BucketController(name=bucket_name_incomplete)
        with pytest.raises(HTTPException) as e:
            bucket_controller.get_data(expiration=1)
        assert e.exconly() == (
            "fastapi.exceptions.HTTPException: 400: "
            "Restoration of requested data still ongoing"
        )
