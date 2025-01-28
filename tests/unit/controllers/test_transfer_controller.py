from fastapi import HTTPException
from icat.entity import Entity
import pytest
from pytest_mock import MockerFixture

from datastore_api.clients.x_root_d_client import get_x_root_d_client
from datastore_api.config import get_settings, Settings
from datastore_api.controllers.transfer_controller import (
    DatasetReArchiver,
    TransferController,
)
from datastore_api.models.dataset import DatasetStatusListFilesResponse
from datastore_api.models.job import JobState
from tests.fixtures import (
    cache_bucket,
    datafile_failed,
    dataset_failed,
    dataset_type,
    facility,
    facility_cycle,
    functional_icat_client,
    instrument,
    investigation,
    investigation_type,
    mock_fts3_settings,
    parameter_type_state,
    submit,
)


class TestDatasetReArchiver:
    @pytest.mark.parametrize(
        ["state", "error"],
        [
            pytest.param(
                JobState.active,
                "fastapi.exceptions.HTTPException: 400: "
                "Archival not yet complete, cannot retry",
            ),
            pytest.param(
                JobState.finished,
                "fastapi.exceptions.HTTPException: 400: "
                "Archival completed successfully, nothing to retry",
            ),
        ],
    )
    def test_validate_status(self, state: JobState, error: str):
        status = DatasetStatusListFilesResponse(state=state, file_states={})
        with pytest.raises(HTTPException) as e:
            DatasetReArchiver._validate_status(status)

        assert e.exconly() == error


class TestTransferController:
    def test_check_source_s3(
        self,
        datafile_failed: Entity,
        cache_bucket: str,
        mocker: MockerFixture,
    ):
        datafile_failed.location = "test"
        transfer_controller = TransferController(
            datafile_entities=[datafile_failed],
            source_key="echo",
            destination_key="rdc",
        )
        transfer_controller.fts3_client.fts3_settings.check_source = True

        transfer_controller._check_source(transfer_controller.datafile_entities[0])

        assert transfer_controller.datafile_entities[0].fileSize == 4

    def test_check_source_s3_failure(
        self,
        mock_fts3_settings: Settings,
        datafile_failed: Entity,
        cache_bucket: str,
        mocker: MockerFixture,
    ):
        settings_copy = mock_fts3_settings.fts3.model_copy()
        settings_copy.check_source = True
        datafile_failed.location = "testtest"
        transfer_controller = TransferController(
            datafile_entities=[datafile_failed],
            source_key="echo",
            destination_key="rdc",
        )
        transfer_controller.fts3_client.fts3_settings = settings_copy

        with pytest.raises(HTTPException) as e:
            transfer_controller._check_source(transfer_controller.datafile_entities[0])

        assert e.exconly() == (
            "fastapi.exceptions.HTTPException: 404: "
            "File not found at cache-bucket/testtest"
        )

    def test_check_source_x_root_d(
        self,
        mock_fts3_settings: Settings,
        datafile_failed: Entity,
        mocker: MockerFixture,
    ):
        get_x_root_d_client.cache_clear()
        settings_copy = mock_fts3_settings.fts3.model_copy()
        settings_copy.check_source = True
        datafile_failed.location = "test"
        transfer_controller = TransferController(
            datafile_entities=[datafile_failed],
            source_key="idc",
            destination_key="rdc",
        )
        transfer_controller.fts3_client.fts3_settings = settings_copy
        mocked_status = mocker.MagicMock()
        mocked_status.code = 0
        mocked_stat_info = mocker.MagicMock()
        mocked_stat_info.size = 4
        mocked_client = mocker.MagicMock()
        mocked_client.stat.return_value = [mocked_status, mocked_stat_info]
        mocked_file_system = mocker.MagicMock()
        mocked_file_system.return_value = mocked_client
        module = "datastore_api.clients.x_root_d_client.client.FileSystem"
        mocker.patch(module, mocked_file_system)

        transfer_controller._check_source(transfer_controller.datafile_entities[0])

        assert transfer_controller.datafile_entities[0].fileSize == 4

    def test_check_source_x_root_d_failure(
        self,
        mock_fts3_settings: Settings,
        datafile_failed: Entity,
        mocker: MockerFixture,
    ):
        get_x_root_d_client.cache_clear()
        settings_copy = mock_fts3_settings.fts3.model_copy()
        settings_copy.check_source = True
        datafile_failed.location = "test"
        transfer_controller = TransferController(
            datafile_entities=[datafile_failed],
            source_key="idc",
            destination_key="rdc",
        )
        transfer_controller.fts3_client.fts3_settings = settings_copy
        mocked_status = mocker.MagicMock()
        mocked_status.code = 400
        mocked_status.message = "[3005] Unable to open directory"
        mocked_client = mocker.MagicMock()
        mocked_client.stat.return_value = [mocked_status]
        mocked_file_system = mocker.MagicMock()
        mocked_file_system.return_value = mocked_client
        module = "datastore_api.clients.x_root_d_client.client.FileSystem"
        mocker.patch(module, mocked_file_system)

        with pytest.raises(HTTPException) as e:
            transfer_controller._check_source(transfer_controller.datafile_entities[0])

        assert e.exconly() == (
            "fastapi.exceptions.HTTPException: 400: [3005] Unable to open directory"
        )

    def test_validate_file_size(self, mock_fts3_settings: Settings):
        settings_copy = mock_fts3_settings.fts3.model_copy()
        settings_copy.file_size_limit = 1
        transfer_controller = TransferController([])
        transfer_controller.fts3_client.fts3_settings = settings_copy
        with pytest.raises(HTTPException) as e:
            transfer_controller._validate_file_size(2)

        assert e.exconly() == (
            "fastapi.exceptions.HTTPException: 400: "
            "Cannot accept file of size 2 due to limit of 1"
        )
        assert transfer_controller.total_size == 2

    def test_validate_total_size(self, mock_fts3_settings: Settings):
        settings_copy = mock_fts3_settings.fts3.model_copy()
        settings_copy.total_file_size_limit = 1
        transfer_controller = TransferController([])
        transfer_controller.fts3_client.fts3_settings = settings_copy
        transfer_controller.total_size = 2
        with pytest.raises(HTTPException) as e:
            transfer_controller._validate_total_size()

        assert e.exconly() == (
            "fastapi.exceptions.HTTPException: 400: "
            "Cannot accept transfer request of total size 2 due to limit of 1"
        )
