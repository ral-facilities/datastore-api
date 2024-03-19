from datetime import datetime
import json

from fastapi.testclient import TestClient
import pytest
from pytest_mock import mocker, MockerFixture

from datastore_api.main import app
from datastore_api.models.archive import (
    ArchiveRequest,
    Facility,
    FacilityCycle,
    Instrument,
    Investigation,
    InvestigationType,
)
from datastore_api.models.restore import RestoreRequest


SESSION_ID = "00000000-0000-0000-0000-000000000000"


@pytest.fixture(scope="function")
def test_client(mocker: MockerFixture):
    icat_client_mock = mocker.patch("datastore_api.main.IcatClient")
    icat_client = icat_client_mock.return_value
    icat_client.login.return_value = SESSION_ID
    icat_client.create_investigations.return_value = ["path/to/data"]
    icat_client.get_investigation_paths.return_value = ["path/to/data"]

    mocker.patch("datastore_api.main.fts3.Context")

    fts_submit_mock = mocker.patch("datastore_api.main.fts3.submit")
    fts_submit_mock.return_value = "0"

    fts_status_mock = mocker.patch("datastore_api.main.fts3.get_job_status")
    fts_status_mock.return_value = {"key": "value"}

    fts_submit_mock = mocker.patch("datastore_api.main.fts3.cancel")
    fts_submit_mock.return_value = "CANCELED"

    return TestClient(app)


class TestMain:
    def test_login(self, test_client: TestClient):
        credentials = {"username": "root", "password": "pw"}
        login_request = {"auth": "simple", "credentials": credentials}
        test_response = test_client.post("/login", content=json.dumps(login_request))

        assert test_response.status_code == 200
        assert json.loads(test_response.content) == {"sessionId": SESSION_ID}

    def test_archive(self, test_client: TestClient):
        investigation = Investigation(
            name="name",
            visitId="visitId",
            title="title",
            summary="summary",
            doi="doi",
            startDate=datetime.now(),
            endDate=datetime.now(),
            releaseDate=datetime.now(),
            facility=Facility(name="facility"),
            investigationType=InvestigationType(name="type"),
            instrument=Instrument(name="instrument"),
            cycle=FacilityCycle(name="20XX"),
        )
        archive_request = ArchiveRequest(investigations=[investigation])
        json_body = json.loads(archive_request.json())
        headers = {"Authorization": f"Bearer {SESSION_ID}"}
        test_response = test_client.post("/archive", headers=headers, json=json_body)

        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert content == {"job_id": "0"}

    def test_restore(self, test_client: TestClient):
        restore_request = RestoreRequest(investigation_ids=[0])
        json_body = json.loads(restore_request.json())
        headers = {"Authorization": f"Bearer {SESSION_ID}"}
        test_response = test_client.post("/restore", headers=headers, json=json_body)

        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert content == {"job_id": "0"}

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
