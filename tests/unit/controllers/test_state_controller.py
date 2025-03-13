from icat.entity import Entity
import pytest
from pytest_mock import MockerFixture

from datastore_api.config import Fts3Settings
from datastore_api.controllers.state_controller import StateController
from datastore_api.models.dataset import (
    DatasetStatusListFilesResponse,
    DatasetStatusResponse,
)
from tests.fixtures import (
    dataset_type,
    dataset_with_job_id,
    facility,
    facility_cycle,
    functional_icat_client,
    instrument,
    investigation,
    investigation_type,
    mock_fts3_settings,
    parameter_type_deletion_date,
    parameter_type_job_ids,
    parameter_type_state,
    SESSION_ID,
    submit,
)


class TestStateController:
    def test_init(
        self,
        mock_fts3_settings: Fts3Settings,
        parameter_type_deletion_date: Entity,
        parameter_type_job_ids: Entity,
        parameter_type_state: Entity,
    ):
        state_controller = StateController(session_id=SESSION_ID)
        assert state_controller.icat_client.client.sessionId == SESSION_ID

    def test_get_dataset_job_ids(
        self,
        mock_fts3_settings: Fts3Settings,
        parameter_type_deletion_date: Entity,
        parameter_type_job_ids: Entity,
        parameter_type_state: Entity,
    ):
        state_controller = StateController()
        assert state_controller.get_dataset_job_ids(dataset_id=1) == []

    def test_get_dataset_datafile_states(
        self,
        mock_fts3_settings: Fts3Settings,
        parameter_type_deletion_date: Entity,
        parameter_type_job_ids: Entity,
        parameter_type_state: Entity,
    ):
        state_controller = StateController()
        assert state_controller.get_datafile_states(dataset_id=1) == []

    @pytest.mark.parametrize(
        ["list_files", "expected_response"],
        [
            pytest.param(False, DatasetStatusResponse(state="SUBMITTED")),
            pytest.param(
                True,
                DatasetStatusListFilesResponse(
                    state="SUBMITTED",
                    file_states={
                        "instrument/20XX/name-visitId/dataset/datafile": "SUBMITTED",
                    },
                ),
            ),
        ],
    )
    def test_get_update_dataset_status(
        self,
        list_files: bool,
        expected_response: DatasetStatusResponse,
        dataset_with_job_id: Entity,
        mocker: MockerFixture,
    ):
        module = "datastore_api.controllers.state_controller.get_fts3_client"
        get_fts3_client_mock = mocker.patch(module)
        get_fts3_client_mock.return_value.statuses.return_value = [
            {
                "job_state": "SUBMITTED",
                "files": [
                    {
                        "file_state": "SUBMITTED",
                        "source_surl": (
                            "root://idc:8446//instrument/20XX/name-visitId/dataset/datafile?query"
                        ),
                    },
                ],
                "job_id": "0",
            },
            {"job_state": "SUBMITTED", "files": [], "job_id": "1"},
            {"job_state": "SUBMITTED", "files": [], "job_id": "2"},
        ]

        state_controller = StateController()
        response = state_controller.get_dataset_status(
            dataset_with_job_id.id,
            list_files=list_files,
        )
        assert response == expected_response

    @pytest.mark.parametrize(
        ["list_files", "expected_response"],
        [
            pytest.param(False, DatasetStatusResponse(state="SUBMITTED")),
            pytest.param(
                True,
                DatasetStatusListFilesResponse(
                    state="SUBMITTED",
                    file_states={
                        "instrument/20XX/name-visitId/dataset/datafile": "SUBMITTED",
                    },
                ),
            ),
        ],
    )
    def test_get_dataset_status(
        self,
        list_files: bool,
        expected_response: DatasetStatusResponse,
        dataset_with_job_id: Entity,
    ):
        state_controller = StateController()
        response = state_controller._get_dataset_status(
            dataset_with_job_id.parameters,
            dataset_with_job_id.id,
            list_files,
        )
        assert response == expected_response
