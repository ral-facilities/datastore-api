from datetime import datetime
import json
import logging
from typing import Generator
from unittest.mock import ANY, MagicMock

from fastapi.testclient import TestClient
from icat.entity import Entity
from icat.query import Query
from pydantic import UUID4
import pytest
from pytest_mock import MockerFixture

from datastore_api.config import Settings
from datastore_api.icat_client import IcatClient
from datastore_api.main import app
from datastore_api.models.archive import (
    ArchiveRequest,
    Datafile,
    Dataset,
    DatasetType,
    Facility,
    FacilityCycle,
    Instrument,
    Investigation,
    InvestigationType,
)
from datastore_api.models.restore import RestoreRequest
from tests.fixtures import (
    dataset_type,
    dataset_with_job_id,
    facility,
    facility_cycle,
    functional_icat_client,
    instrument,
    investigation,
    investigation_tear_down,
    investigation_type,
    mock_fts3_settings,
    parameter_type_job_ids,
    parameter_type_state,
    SESSION_ID,
    submit,
)

log = logging.getLogger("tests")


@pytest.fixture(scope="function")
def test_client(mock_fts3_settings: Settings) -> TestClient:
    return TestClient(app)


@pytest.fixture(scope="function")
def session_id(test_client: TestClient) -> Generator[str, None, None]:
    credentials = {"username": "root", "password": "pw"}
    login_request = {"auth": "simple", "credentials": credentials}
    response = test_client.post("/login", content=json.dumps(login_request))

    session_id = json.loads(response.content)["sessionId"]

    yield session_id


def fts_job(
    sources: list[str],
    destinations: list[str],
    bring_online: int = -1,
    archive_timeout: int = -1,
) -> dict:
    return {
        "files": [
            {
                "sources": sources,
                "destinations": destinations,
                "checksum": "ADLER32",
                "selection_strategy": "auto",
            },
        ],
        "delete": None,
        "params": {
            "verify_checksum": "none",
            "reuse": None,
            "spacetoken": None,
            "bring_online": bring_online,
            "dst_file_report": False,
            "archive_timeout": archive_timeout,
            "copy_pin_lifetime": None,
            "job_metadata": None,
            "source_spacetoken": None,
            "overwrite": False,
            "overwrite_on_retry": False,
            "overwrite_hop": False,
            "multihop": False,
            "retry": -1,
            "retry_delay": 0,
            "priority": None,
            "strict_copy": False,
            "max_time_in_queue": None,
            "timeout": None,
            "id_generator": "standard",
            "sid": None,
            "s3alternate": False,
            "nostreams": 1,
            "buffer_size": None,
        },
    }


class TestLogin:
    def test_login_success(self, test_client: TestClient):
        credentials = {"username": "root", "password": "pw"}
        login_request = {"auth": "simple", "credentials": credentials}
        test_response = test_client.post("/login", content=json.dumps(login_request))

        assert test_response.status_code == 200
        assert list(json.loads(test_response.content).keys()) == ["sessionId"]

    @pytest.mark.parametrize(
        "login_request, detail",
        [
            pytest.param(
                {
                    "auth": "simple",
                    "credentials": {"username": "root", "password": "p"},
                },
                "The username and password do not match ",
                id="Bad credentials",
            ),
            pytest.param(
                {
                    "auth": "simpl",
                    "credentials": {"username": "root", "password": "pw"},
                },
                "Authenticator mnemonic simpl not recognised",
                id="Bad auth",
            ),
        ],
    )
    def test_login_failure(
        self,
        test_client: TestClient,
        login_request: dict,
        detail: str,
    ):
        test_response = test_client.post("/login", content=json.dumps(login_request))

        assert test_response.status_code == 401
        assert json.loads(test_response.content)["detail"] == detail


class TestArchive:
    def test_archive(
        self,
        test_client: TestClient,
        submit: MagicMock,
        session_id: str,
        facility: Entity,
        investigation_type: Entity,
        dataset_type: Entity,
        facility_cycle: Entity,
        instrument: Entity,
        investigation: Entity,
        parameter_type_state: Entity,
        parameter_type_job_ids: Entity,
    ):
        dataset = Dataset(
            name="dataset1",
            datasetType=DatasetType(name="type"),
            datafiles=[Datafile(name="datafile")],
        )
        investigation_metadata = Investigation(
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
            facilityCycle=FacilityCycle(name="20XX"),
            datasets=[dataset],
        )
        archive_request = ArchiveRequest(investigations=[investigation_metadata])
        json_body = json.loads(archive_request.json())
        headers = {"Authorization": f"Bearer {session_id}"}
        test_response = test_client.post("/archive", headers=headers, json=json_body)

        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert "job_ids" in content
        assert len(content["job_ids"]) == 1
        UUID4(content["job_ids"][0])

        path = "/instrument/20XX/name-visitId/type/dataset1/datafile"
        sources = [f"root://idc:1094/{path}", f"root://udc:1094/{path}"]
        destinations = [f"root://archive:1094/{path}"]
        job = fts_job(
            sources=sources,
            destinations=destinations,
            archive_timeout=28800,
        )
        submit.assert_called_once_with(context=ANY, job=job)

        icat_client = IcatClient(session_id=session_id)
        query = Query(
            client=icat_client.client,
            entity="Investigation",
            conditions={"name": " = 'name'"},
            includes=[
                "facility",
                "type",
                "investigationInstruments.instrument",
                "investigationFacilityCycles.facilityCycle",
                "datasets.type",
                "datasets.datafiles",
            ],
        )
        investigations = icat_client.client.search(query=query)
        assert len(investigations) == 1
        investigation_entity = investigations[0]

        investigation_instruments = investigation_entity.investigationInstruments
        assert len(investigation_instruments) == 1

        investigation_cycles = investigation_entity.investigationFacilityCycles
        assert len(investigation_cycles) == 1

        assert len(investigation_entity.datasets) == 2
        assert len(investigation_entity.datasets[0].datafiles) == 1
        assert len(investigation_entity.datasets[1].datafiles) == 1

        assert investigation_entity.name == "name"
        assert investigation_entity.visitId == "visitId"
        assert investigation_entity.title == "title"
        assert investigation_entity.summary == "summary"
        assert investigation_entity.startDate is not None
        assert investigation_entity.endDate is not None
        assert investigation_entity.releaseDate is not None
        assert investigation_entity.facility.name == "facility"
        assert investigation_entity.type.name == "type"
        assert investigation_instruments[0].instrument.name == "instrument"
        assert investigation_cycles[0].facilityCycle.name == "20XX"
        assert investigation_entity.datasets[0].name == "dataset"
        assert investigation_entity.datasets[0].type.name == "type"
        assert investigation_entity.datasets[0].datafiles[0].name == "datafile"
        assert investigation_entity.datasets[1].name == "dataset1"
        assert investigation_entity.datasets[1].type.name == "type"
        assert investigation_entity.datasets[1].datafiles[0].name == "datafile"

    def test_archive_new_investigation(
        self,
        test_client: TestClient,
        submit: MagicMock,
        session_id: str,
        facility: Entity,
        investigation_type: Entity,
        dataset_type: Entity,
        facility_cycle: Entity,
        instrument: Entity,
        parameter_type_state: Entity,
        parameter_type_job_ids: Entity,
        investigation_tear_down: None,
    ):
        dataset = Dataset(
            name="dataset1",
            datasetType=DatasetType(name="type"),
            datafiles=[Datafile(name="datafile")],
        )
        investigation_metadata = Investigation(
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
            facilityCycle=FacilityCycle(name="20XX"),
            datasets=[dataset],
        )
        archive_request = ArchiveRequest(investigations=[investigation_metadata])
        json_body = json.loads(archive_request.json())
        headers = {"Authorization": f"Bearer {session_id}"}
        test_response = test_client.post("/archive", headers=headers, json=json_body)

        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert "job_ids" in content
        assert len(content["job_ids"]) == 1
        UUID4(content["job_ids"][0])

        path = "/instrument/20XX/name-visitId/type/dataset1/datafile"
        sources = [f"root://idc:1094/{path}", f"root://udc:1094/{path}"]
        destinations = [f"root://archive:1094/{path}"]
        job = fts_job(
            sources=sources,
            destinations=destinations,
            archive_timeout=28800,
        )
        submit.assert_called_once_with(context=ANY, job=job)

        icat_client = IcatClient(session_id=session_id)
        query = Query(
            client=icat_client.client,
            entity="Investigation",
            conditions={"name": " = 'name'"},
            includes=[
                "facility",
                "type",
                "investigationInstruments.instrument",
                "investigationFacilityCycles.facilityCycle",
                "datasets.type",
                "datasets.datafiles",
            ],
        )
        investigations = icat_client.client.search(query=query)
        assert len(investigations) == 1
        investigation_entity = investigations[0]

        investigation_instruments = investigation_entity.investigationInstruments
        assert len(investigation_instruments) == 1

        investigation_cycles = investigation_entity.investigationFacilityCycles
        assert len(investigation_cycles) == 1

        assert len(investigation_entity.datasets) == 1
        assert len(investigation_entity.datasets[0].datafiles) == 1

        assert investigation_entity.name == "name"
        assert investigation_entity.visitId == "visitId"
        assert investigation_entity.title == "title"
        assert investigation_entity.summary == "summary"
        assert investigation_entity.startDate is not None
        assert investigation_entity.endDate is not None
        assert investigation_entity.releaseDate is not None
        assert investigation_entity.facility.name == "facility"
        assert investigation_entity.type.name == "type"
        assert investigation_instruments[0].instrument.name == "instrument"
        assert investigation_cycles[0].facilityCycle.name == "20XX"
        assert investigation_entity.datasets[0].name == "dataset1"
        assert investigation_entity.datasets[0].type.name == "type"
        assert investigation_entity.datasets[0].datafiles[0].name == "datafile"


class TestRestore:
    @pytest.mark.parametrize(
        ["restore_ids"],
        [
            pytest.param("investigation_ids"),
            pytest.param("dataset_ids"),
            pytest.param("datafile_ids"),
        ],
    )
    def test_restore(
        self,
        submit: MagicMock,
        session_id: str,
        facility: Entity,
        investigation_type: Entity,
        facility_cycle: Entity,
        instrument: Entity,
        investigation: Entity,
        functional_icat_client: IcatClient,
        test_client: TestClient,
        restore_ids: str,
    ):
        if restore_ids == "investigation_ids":
            restore_request = RestoreRequest(investigation_ids=[investigation.id])
        elif restore_ids == "dataset_ids":
            equals = {"investigation.id": investigation.id}
            dataset = functional_icat_client.get_single_entity("Dataset", equals)
            restore_request = RestoreRequest(dataset_ids=[dataset.id])
        elif restore_ids == "datafile_ids":
            equals = {"dataset.investigation.id": investigation.id}
            datafile = functional_icat_client.get_single_entity("Datafile", equals)
            restore_request = RestoreRequest(datafile_ids=[datafile.id])

        json_body = json.loads(restore_request.json())
        headers = {"Authorization": f"Bearer {session_id}"}
        test_response = test_client.post("/restore", headers=headers, json=json_body)

        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert "job_ids" in content
        assert len(content["job_ids"]) == 1
        UUID4(content["job_ids"][0])

        path = "instrument/20XX/name-visitId/type/dataset/datafile"
        sources = [f"root://archive:1094//{path}"]
        destinations = [f"root://udc:1094//{path}"]
        job = fts_job(
            sources=sources,
            destinations=destinations,
            bring_online=28800,
        )
        submit.assert_called_once_with(context=ANY, job=job)


class TestCancel:
    def test_cancel(
        self,
        test_client: TestClient,
        mocker: MockerFixture,
    ):
        fts_submit_mock = mocker.patch("datastore_api.fts3_client.fts3.cancel")
        fts_submit_mock.return_value = "SUBMITTED"
        test_response = test_client.delete("/job/0")

        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert content == {"state": "SUBMITTED"}

    def test_cancel_archival(
        self,
        test_client: TestClient,
        dataset_with_job_id: Entity,
    ):
        test_response = test_client.delete("/job/1")

        content = json.loads(test_response.content)
        assert test_response.status_code == 400, content
        assert content == {"detail": "Archival jobs cannot be cancelled"}
