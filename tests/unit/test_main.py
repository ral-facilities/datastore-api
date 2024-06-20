import json

from fastapi.testclient import TestClient
from pydantic import UUID4
import pytest
from pytest_mock import mocker, MockerFixture

from datastore_api.config import Settings
from datastore_api.main import app
from datastore_api.models.archive import ArchiveRequest, Investigation
from datastore_api.models.restore import RestoreRequest
from tests.fixtures import investigation_metadata, mock_fts3_settings, submit


SESSION_ID = "00000000-0000-0000-0000-000000000000"
FILES = [
    {
        "file_state": "FINISHED",
        "dest_surl": "mock://test.cern.ch/ttqv/pryb/nnvw?size_post=1048576&time=2",
    },
    {
        "file_state": "FAILED",
        "dest_surl": "mock://test.cern.ch/swnx/jznu/laso?size_post=1048576&time=2",
    },
]
STATUSES = [{"job_state": "FINISHEDDIRTY", "files": FILES}]


@pytest.fixture(scope="function")
def test_client(mock_fts3_settings: Settings, mocker: MockerFixture):
    icat_client_mock = mocker.patch("datastore_api.main.IcatClient")
    icat_client = icat_client_mock.return_value
    icat_client.settings = mock_fts3_settings.icat
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
    fts_submit_mock.return_value = SESSION_ID

    fts_status_mock = mocker.patch("datastore_api.fts3_client.fts3.get_jobs_statuses")
    fts_status_mock.return_value = STATUSES

    fts_cancel_mock = mocker.patch("datastore_api.fts3_client.fts3.cancel")
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
        investigation_metadata: Investigation,
    ):
        archive_request = ArchiveRequest(investigations=[investigation_metadata])
        json_body = json.loads(archive_request.json())
        headers = {"Authorization": f"Bearer {SESSION_ID}"}
        test_response = test_client.post("/archive", headers=headers, json=json_body)

        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert "job_ids" in content
        assert len(content["job_ids"]) == 1
        UUID4(content["job_ids"][0])

    def test_restore_to_udc(self, test_client: TestClient):
        restore_request = RestoreRequest(investigation_ids=[0])
        json_body = json.loads(restore_request.json())
        headers = {"Authorization": f"Bearer {SESSION_ID}"}
        test_response = test_client.post(
            "/restore/udc",
            headers=headers,
            json=json_body,
        )

        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert "job_ids" in content
        assert len(content["job_ids"]) == 1
        UUID4(content["job_ids"][0])

    def test_restore_to_download(self, test_client: TestClient):
        restore_request = RestoreRequest(investigation_ids=[0])
        json_body = json.loads(restore_request.json())
        headers = {"Authorization": f"Bearer {SESSION_ID}"}
        test_response = test_client.post(
            "/restore/download",
            headers=headers,
            json=json_body,
        )

        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert "job_ids" in content
        assert len(content["job_ids"]) == 1
        UUID4(content["job_ids"][0])

    def test_get_data(self, test_client: TestClient):
        headers = {"Authorization": f"Bearer {SESSION_ID}"}
        test_response = test_client.get("/data?job_ids=1&job_ids=2", headers=headers)

        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert "nnvw" in content
        assert "laso" in content

    def test_status(self, test_client: TestClient):
        headers = {"Authorization": f"Bearer {SESSION_ID}"}
        test_response = test_client.get("/job/1", headers=headers)

        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert content == {"status": STATUSES[0]}

    def test_complete(self, test_client: TestClient):
        headers = {"Authorization": f"Bearer {SESSION_ID}"}
        test_response = test_client.get("/job/1/complete", headers=headers)

        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert content == {"complete": True}

    def test_percentage(self, test_client: TestClient):
        headers = {"Authorization": f"Bearer {SESSION_ID}"}
        test_response = test_client.get("/job/1/percentage", headers=headers)
        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert content == {"percentage_complete": 100.0}

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
