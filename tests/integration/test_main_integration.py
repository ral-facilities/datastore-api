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
from datastore_api.icat_client import get_icat_cache, IcatClient
from datastore_api.main import app
from datastore_api.models.archive import ArchiveRequest
from datastore_api.models.icat import (
    Investigation,
    InvestigationTypeIdentifier,
)
from datastore_api.models.restore import RestoreRequest
from tests.fixtures import (
    archive_request,
    archive_request_parameters,
    archive_request_sample,
    bucket_deletion,
    datafile_format,
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
    parameter_type_date_time,
    parameter_type_job_ids,
    parameter_type_numeric,
    parameter_type_state,
    parameter_type_string,
    sample_type,
    SESSION_ID,
    submit,
    technique,
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
    strict_copy: bool = False,
) -> dict:
    return {
        "files": [
            {
                "sources": sources,
                "destinations": destinations,
                "selection_strategy": "auto",
            },
        ],
        "delete": None,
        "params": {
            "verify_checksum": "none",
            "reuse": None,
            "destination_spacetoken": None,
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
            "strict_copy": strict_copy,
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
        parameter_type_string: Entity,
        parameter_type_numeric: Entity,
        parameter_type_date_time: Entity,
        sample_type: Entity,
        technique: Entity,
        datafile_format: Entity,
        archive_request: ArchiveRequest,
    ):
        get_icat_cache.cache_clear()
        json_body = json.loads(archive_request.json())
        headers = {"Authorization": f"Bearer {session_id}"}
        test_response = test_client.post("/archive", headers=headers, json=json_body)

        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert "job_ids" in content
        assert len(content["job_ids"]) == 1
        UUID4(content["job_ids"][0])

        path = "/instrument/20XX/name-visitId/type/dataset1/datafile"
        sources = [f"root://idc.ac.uk:1094/{path}", f"root://rdc.ac.uk:1094/{path}"]
        destinations = [f"root://archive.ac.uk:1094/{path}"]
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
                "datasets.sample.type",
                "datasets.sample.parameters.type",
                "datasets.datasetTechniques.technique",
                "datasets.datasetInstruments.instrument",
                "datasets.parameters.type",
                "datasets.datafiles.datafileFormat",
                "datasets.datafiles.parameters.type",
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

        dataset = investigation_entity.datasets[1]
        parameters = sorted(dataset.parameters, key=lambda x: x.type.name)
        sample_parameters = sorted(dataset.sample.parameters, key=lambda x: x.type.name)
        dataset_location = "instrument/20XX/name-visitId/type/dataset1"
        assert dataset.name == "dataset1"
        assert dataset.location == dataset_location
        assert dataset.type.name == "type"
        assert dataset.sample.name == "sample"
        assert dataset.sample.type.name == "carbon"
        assert dataset.sample.type.molecularFormula == "C"
        assert len(sample_parameters) == 3
        assert sample_parameters[0].type.name == "date_time"
        assert sample_parameters[0].dateTimeValue is not None
        assert sample_parameters[1].type.name == "numeric"
        assert sample_parameters[1].numericValue == 0
        assert sample_parameters[1].error == 0
        assert sample_parameters[1].rangeBottom == -1
        assert sample_parameters[1].rangeTop == 1
        assert sample_parameters[2].type.name == "string"
        assert sample_parameters[2].stringValue == "stringValue"
        assert len(dataset.datasetInstruments) == 1
        assert dataset.datasetInstruments[0].instrument.name == "instrument"
        assert len(dataset.datasetTechniques) == 1
        assert dataset.datasetTechniques[0].technique.name == "technique"
        assert len(parameters) == 5
        assert parameters[0].type.name == "Archival ids"
        assert parameters[0].stringValue is not None
        assert parameters[1].type.name == "Archival state"
        assert parameters[1].stringValue == "SUBMITTED"
        assert parameters[2].type.name == "date_time"
        assert parameters[2].dateTimeValue is not None
        assert parameters[3].type.name == "numeric"
        assert parameters[3].numericValue == 0
        assert parameters[3].error == 0
        assert parameters[3].rangeBottom == -1
        assert parameters[3].rangeTop == 1
        assert parameters[4].type.name == "string"
        assert parameters[4].stringValue == "stringValue"

        datafile = investigation_entity.datasets[1].datafiles[0]
        parameters = sorted(datafile.parameters, key=lambda x: x.type.name)
        assert datafile.name == "datafile"
        assert datafile.location == dataset_location + "/datafile"
        assert datafile.datafileFormat.name == "txt"
        assert datafile.datafileFormat.version == "0"
        assert len(parameters) == 4
        assert parameters[0].type.name == "Archival state"
        assert parameters[0].stringValue == "SUBMITTED"
        assert parameters[1].type.name == "date_time"
        assert parameters[1].dateTimeValue is not None
        assert parameters[2].type.name == "numeric"
        assert parameters[2].numericValue == 0
        assert parameters[2].error == 0
        assert parameters[2].rangeBottom == -1
        assert parameters[2].rangeTop == 1
        assert parameters[3].type.name == "string"
        assert parameters[3].stringValue == "stringValue"

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
        parameter_type_string: Entity,
        parameter_type_numeric: Entity,
        parameter_type_date_time: Entity,
        sample_type: Entity,
        technique: Entity,
        datafile_format: Entity,
        investigation_tear_down: None,
        archive_request: ArchiveRequest,
    ):
        get_icat_cache.cache_clear()
        archive_request.investigation_identifier = Investigation(
            title="title",
            summary="summary",
            startDate=datetime.now(),
            endDate=datetime.now(),
            releaseDate=datetime.now(),
            investigationType=InvestigationTypeIdentifier(name="type"),
            facilityCycle=archive_request.facility_cycle_identifier,
            instrument=archive_request.instrument_identifier,
            datasets=[archive_request.dataset],
            **archive_request.investigation_identifier.dict(),
        )
        json_body = json.loads(archive_request.json())
        headers = {"Authorization": f"Bearer {session_id}"}
        test_response = test_client.post("/archive", headers=headers, json=json_body)

        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert "job_ids" in content
        assert len(content["job_ids"]) == 1
        UUID4(content["job_ids"][0])

        path = "/instrument/20XX/name-visitId/type/dataset1/datafile"
        sources = [f"root://idc.ac.uk:1094/{path}", f"root://rdc.ac.uk:1094/{path}"]
        destinations = [f"root://archive.ac.uk:1094/{path}"]
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
    def test_restore_rdc(
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
        test_response = test_client.post(
            "/restore/rdc",
            headers=headers,
            json=json_body,
        )

        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert "job_ids" in content
        assert len(content["job_ids"]) == 1
        UUID4(content["job_ids"][0])

        path = "instrument/20XX/name-visitId/type/dataset/datafile"
        sources = [f"root://archive.ac.uk:1094//{path}"]
        destinations = [f"root://rdc.ac.uk:1094//{path}"]
        job = fts_job(
            sources=sources,
            destinations=destinations,
            bring_online=28800,
        )
        submit.assert_called_once_with(context=ANY, job=job)

    @pytest.mark.parametrize(
        ["restore_ids"],
        [
            pytest.param("investigation_ids"),
            pytest.param("dataset_ids"),
            pytest.param("datafile_ids"),
        ],
    )
    def test_restore_download(
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
        bucket_deletion: None,
    ):
        if restore_ids == "investigation_ids":
            restore_request = RestoreRequest(
                investigation_ids=[investigation.id],
            )
        elif restore_ids == "dataset_ids":
            equals = {"investigation.id": investigation.id}
            dataset = functional_icat_client.get_single_entity("Dataset", equals)
            restore_request = RestoreRequest(
                dataset_ids=[dataset.id],
            )
        elif restore_ids == "datafile_ids":
            equals = {"dataset.investigation.id": investigation.id}
            datafile = functional_icat_client.get_single_entity("Datafile", equals)
            restore_request = RestoreRequest(
                datafile_ids=[datafile.id],
            )

        json_body = json.loads(restore_request.json())
        headers = {"Authorization": f"Bearer {session_id}"}
        test_response = test_client.post(
            "/restore/download",
            headers=headers,
            json=json_body,
        )
        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert "job_ids" in content
        assert len(content["job_ids"]) == 1
        assert "bucket_name" in content
        UUID4(content["job_ids"][0])
        UUID4(content["bucket_name"])

        bucket_name = content["bucket_name"]
        path = "instrument/20XX/name-visitId/type/dataset/datafile"
        sources = [f"root://archive.ac.uk:1094//{path}?copy_mode=push"]
        destinations = [
            f"s3s://127.0.0.1:9000/{bucket_name}/{path}",
        ]
        job = fts_job(
            sources=sources,
            destinations=destinations,
            bring_online=28800,
            strict_copy=True,
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
