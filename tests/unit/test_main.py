from datetime import datetime
import json

from fastapi.testclient import TestClient
import pytest
from pytest_mock import mocker, MockerFixture

from datastore_api.config import get_settings
from datastore_api.main import app
from datastore_api.models.archive import ArchiveRequest, Investigation
from datastore_api.models.restore import RestoreRequest
from fixtures import investigation


SESSION_ID = "00000000-0000-0000-0000-000000000000"


@pytest.fixture(scope="function")
def test_client(mocker: MockerFixture):
    icat_client_mock = mocker.patch("datastore_api.main.IcatClient")
    icat_client = icat_client_mock.return_value
    icat_client.icat_settings = get_settings().icat
    icat_client.login.return_value = SESSION_ID
    icat_client.get_paths.return_value = ["path/to/data"]
    icat_client.check_job_id.return_value = None

    dataset = mocker.MagicMock(name="dataset")
    dataset_parameter_state = mocker.MagicMock(name="dataset_parameter_state")
    dataset_parameter_state.type.name = "Archival state"
    dataset_parameter_job_ids = mocker.MagicMock(name="dataset_parameter_job_ids")
    dataset_parameter_job_ids.type.name = "Archival ids"
    dataset.parameters = [dataset_parameter_state, dataset_parameter_job_ids]
    icat_client.new_dataset.return_value = dataset, ["path/to/data"]

    mocker.patch("datastore_api.fts3_client.fts3.Context")

    fts_submit_mock = mocker.patch("datastore_api.fts3_client.fts3.submit")
    fts_submit_mock.return_value = "0"

    fts_status_mock = mocker.patch("datastore_api.fts3_client.fts3.get_job_status")
    fts_status_mock.return_value = {"key": "value"}

    fts_submit_mock = mocker.patch("datastore_api.fts3_client.fts3.cancel")
    fts_submit_mock.return_value = "CANCELED"

    return TestClient(app)


class TestMain:
    def test_login(self, test_client: TestClient):
        credentials = {"username": "root", "password": "pw"}
        login_request = {"auth": "simple", "credentials": credentials}
        test_response = test_client.post("/login", content=json.dumps(login_request))

        assert test_response.status_code == 200
        assert json.loads(test_response.content) == {"sessionId": SESSION_ID}

    def test_archive(self, test_client: TestClient, investigation: Investigation):
        archive_request = ArchiveRequest(investigations=[investigation])
        json_body = json.loads(archive_request.json())
        headers = {"Authorization": f"Bearer {SESSION_ID}"}
        test_response = test_client.post("/archive", headers=headers, json=json_body)

        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert content == {"job_ids": ["0"]}

    def test_restore(self, test_client: TestClient):
        restore_request = RestoreRequest(investigation_ids=[0])
        json_body = json.loads(restore_request.json())
        headers = {"Authorization": f"Bearer {SESSION_ID}"}
        test_response = test_client.post("/restore", headers=headers, json=json_body)

        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert content == {"job_ids": ["0"]}

    def test_status(self, test_client: TestClient):
        headers = {"Authorization": f"Bearer {SESSION_ID}"}
        test_response = test_client.get("/job/1", headers=headers)

        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert content == {"status": {"key": "value"}}

    def test_cancel(self, test_client: TestClient):
        headers = {"Authorization": f"Bearer {SESSION_ID}"}
        test_response = test_client.delete("/job/1", headers=headers)

        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert content == {"state": "CANCELED"}

    def test_version(self, test_client: TestClient):
        test_response = test_client.get("/version")

        assert test_response.status_code == 200
        assert json.loads(test_response.content) == {"version": "0.1.0"}
