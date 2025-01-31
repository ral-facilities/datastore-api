import json
from uuid import UUID

from fastapi.testclient import TestClient
import pytest
from pytest_mock import mocker, MockerFixture

from datastore_api.config import Settings
from datastore_api.main import app
from datastore_api.models.archive import ArchiveRequest
from datastore_api.models.dataset import DatasetStatusResponse
from datastore_api.models.transfer import TransferRequest
from tests.fixtures import (
    archive_request,
    archive_request_parameters,
    archive_request_sample,
    FILES,
    mock_fts3_settings,
    SESSION_ID,
    STATUSES,
    submit,
)


@pytest.fixture(scope="function")
def test_client(mock_fts3_settings: Settings, mocker: MockerFixture):
    datafile = mocker.MagicMock(name="datafile")
    datafile.fileSize = None
    dataset = mocker.MagicMock(name="dataset")
    dataset_parameter_state = mocker.MagicMock(name="dataset_parameter_state")
    dataset_parameter_state.type.name = "Archival state"
    dataset_parameter_job_ids = mocker.MagicMock(name="dataset_parameter_job_ids")
    dataset_parameter_job_ids.type.name = "Archival ids"
    dataset.parameters = [dataset_parameter_state, dataset_parameter_job_ids]
    dataset.datafiles = [datafile]

    for module in {"main", "controllers.state_controller"}:
        icat_client_mock = mocker.patch(f"datastore_api.{module}.IcatClient")
        icat_client = icat_client_mock.return_value
        icat_client.settings = mock_fts3_settings.icat
        icat_client.login.return_value = SESSION_ID
        icat_client.get_unique_datafiles.return_value = [datafile]
        icat_client.check_job_id.return_value = None
        icat_client.new_dataset.return_value = dataset
        icat_client.create_many.return_value = {1}

    mocker.patch("datastore_api.clients.fts3_client.fts3.Context")

    fts_submit_mock = mocker.patch("datastore_api.clients.fts3_client.fts3.submit")
    fts_submit_mock.return_value = SESSION_ID

    module = "datastore_api.clients.fts3_client.fts3.get_job_status"
    fts_status_mock = mocker.patch(module)
    fts_status_mock.return_value = STATUSES[0]

    module = "datastore_api.clients.fts3_client.fts3.get_jobs_statuses"
    fts_status_mock = mocker.patch(module)
    fts_status_mock.return_value = STATUSES

    fts_cancel_mock = mocker.patch("datastore_api.clients.fts3_client.fts3.cancel")
    fts_cancel_mock.return_value = "CANCELED"

    return TestClient(app)


class TestMain:
    def test_login(self, test_client: TestClient):
        credentials = {"username": "root", "password": "pw"}
        login_request = {"auth": "simple", "credentials": credentials}
        test_response = test_client.post("/login", content=json.dumps(login_request))

        assert test_response.status_code == 200
        assert json.loads(test_response.content) == {"sessionId": SESSION_ID}

    def test_archive(
        self,
        test_client: TestClient,
        archive_request: ArchiveRequest,
    ):
        json_body = json.loads(archive_request.model_dump_json(exclude_none=True))
        headers = {"Authorization": f"Bearer {SESSION_ID}"}
        test_response = test_client.post(
            "/archive/idc",
            headers=headers,
            json=json_body,
        )

        assert test_response.status_code == 200, test_response.content
        content = json.loads(test_response.content)
        assert "dataset_ids" in content
        assert content["dataset_ids"] == [1]
        assert "job_ids" in content
        assert len(content["job_ids"]) == 1
        UUID(content["job_ids"][0], version=4)

    def test_restore_to_rdc(self, test_client: TestClient):
        restore_request = TransferRequest(investigation_ids=[0])
        json_body = json.loads(restore_request.model_dump_json(exclude_none=True))
        headers = {"Authorization": f"Bearer {SESSION_ID}"}
        test_response = test_client.post(
            "/restore/rdc",
            headers=headers,
            json=json_body,
        )

        assert test_response.status_code == 200, test_response.content
        content = json.loads(test_response.content)
        assert "job_ids" in content
        assert len(content["job_ids"]) == 1
        UUID(content["job_ids"][0], version=4)

    def test_get_dataset_without_update(
        self,
        test_client: TestClient,
        mocker: MockerFixture,
    ):
        response = DatasetStatusResponse(state="FINISHED")
        state_controller_mock = mocker.patch("datastore_api.main.StateController")
        state_controller = state_controller_mock.return_value
        state_controller.get_dataset_job_ids.return_value = []
        state_controller.get_dataset_status.return_value = response

        headers = {"Authorization": f"Bearer {SESSION_ID}"}
        test_response = test_client.get(
            "/dataset/1/status",
            params={"list_files": False},
            headers=headers,
        )

        assert test_response.status_code == 200, test_response.content
        content = json.loads(test_response.content)
        assert content == {"state": "FINISHED"}

    def test_status(self, test_client: TestClient):
        headers = {"Authorization": f"Bearer {SESSION_ID}"}
        test_response = test_client.get("/job/1/status", headers=headers)

        assert test_response.status_code == 200, test_response.content
        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert content == {
            "status": STATUSES[0],
        }

    def test_status_multiple(self, test_client: TestClient):
        headers = {"Authorization": f"Bearer {SESSION_ID}"}
        test_response = test_client.get(
            "/job/1/status?list_files=true&verbose=false",
            headers=headers,
        )

        content = json.loads(test_response.content)

        assert test_response.status_code == 200, content
        assert content == {
            "state": STATUSES[0]["job_state"],
            "file_states": {
                "test0": FILES[0]["file_state"],
                "test1": FILES[1]["file_state"],
            },
        }

    def test_status_multiple1(self, test_client: TestClient):
        headers = {"Authorization": f"Bearer {SESSION_ID}"}
        test_response = test_client.get(
            "/job/1/status?list_files=false&verbose=false",
            headers=headers,
        )

        content = json.loads(test_response.content)

        assert test_response.status_code == 200, content
        assert content == {
            "state": STATUSES[0]["job_state"],
        }

    def test_complete(self, test_client: TestClient):
        headers = {"Authorization": f"Bearer {SESSION_ID}"}
        test_response = test_client.get("/job/1/complete", headers=headers)

        assert test_response.status_code == 200, test_response.content
        content = json.loads(test_response.content)
        assert content == {"complete": True}

    def test_percentage(self, test_client: TestClient):
        headers = {"Authorization": f"Bearer {SESSION_ID}"}
        test_response = test_client.get("/job/1/percentage", headers=headers)
        assert test_response.status_code == 200, test_response.content
        content = json.loads(test_response.content)
        assert content == {"percentage_complete": 100.0}

    def test_cancel(self, test_client: TestClient):
        headers = {"Authorization": f"Bearer {SESSION_ID}"}
        test_response = test_client.delete("/job/1", headers=headers)

        assert test_response.status_code == 200, test_response.content
        content = json.loads(test_response.content)
        assert content == {"state": "CANCELED"}

    def test_version(self, test_client: TestClient):
        test_response = test_client.get("/version")

        assert test_response.status_code == 200
        assert json.loads(test_response.content) == {"version": "0.1.0"}

    def test_get_storage_info(self, test_client: TestClient):
        test_response = test_client.get("/storage-type")
        content = json.loads(test_response.content)

        assert test_response.status_code == 200, content
        assert content == {
            "archive": "tape",
            "storage": {"echo": "s3", "idc": "disk", "rdc": "disk"},
        }
