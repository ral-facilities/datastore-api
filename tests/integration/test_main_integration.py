from datetime import datetime
import json
import logging
from typing import Generator
from unittest.mock import ANY, MagicMock
from uuid import UUID

from fastapi.testclient import TestClient
from fts3.rest.client.exceptions import ServerError
from icat.entity import Entity
from icat.query import Query
import pytest
from pytest_mock import MockerFixture

from datastore_api.clients.icat_client import get_icat_cache, IcatClient
from datastore_api.config import Settings
from datastore_api.main import app
from datastore_api.models.archive import ArchiveRequest
from datastore_api.models.icat import (
    FacilityCycleIdentifier,
    InstrumentIdentifier,
    Investigation,
    InvestigationTypeIdentifier,
)
from datastore_api.models.transfer import BucketAcl, TransferRequest, TransferS3Request
from tests.fixtures import (
    archive_request,
    archive_request_parameters,
    archive_request_sample,
    bucket_deletion,
    bucket_name_private,
    cache_bucket,
    datafile_failed,
    datafile_format,
    dataset_failed,
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
    mock_fts3_settings_no_archive,
    parameter_type_date_time,
    parameter_type_deletion_date,
    parameter_type_job_ids,
    parameter_type_numeric,
    parameter_type_state,
    parameter_type_string,
    sample_type,
    SESSION_ID,
    submit,
    technique,
)
from tests.unit.test_main import STATUSES

log = logging.getLogger("tests")


@pytest.fixture(scope="function")
def test_client(mock_fts3_settings: Settings) -> TestClient:
    return TestClient(app)


@pytest.fixture(scope="function")
def test_client_no_archive(mock_fts3_settings_no_archive: Settings) -> TestClient:
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
            "disable_cleanup": False,
            "overwrite_when_only_on_disk": False,
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
    @pytest.mark.flaky(only_on=[ServerError])
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
        dataset_failed: Entity,
        datafile_failed: Entity,
        parameter_type_state: Entity,
        parameter_type_job_ids: Entity,
        parameter_type_deletion_date: Entity,
        parameter_type_string: Entity,
        parameter_type_numeric: Entity,
        parameter_type_date_time: Entity,
        sample_type: Entity,
        technique: Entity,
        datafile_format: Entity,
        archive_request: ArchiveRequest,
    ):
        get_icat_cache.cache_clear()
        json_body = json.loads(archive_request.model_dump_json(exclude_none=True))
        headers = {"Authorization": f"Bearer {session_id}"}
        test_response = test_client.post(
            "/archive/idc",
            headers=headers,
            json=json_body,
        )

        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert "job_ids" in content
        assert len(content["job_ids"]) == 1
        UUID(content["job_ids"][0], version=4)

        path = "/instrument/20XX/name-visitId/type/dataset1/datafile"
        sources = [f"root://idc.ac.uk:1094/{path}"]
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

        test_response = test_client.get(f"/job/{content['job_ids'][0]}/status")
        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert "status" in content
        assert isinstance(content["status"], dict)

    @pytest.mark.flaky(only_on=[ServerError], retries=3)
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
        parameter_type_deletion_date: Entity,
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
            facilityCycle=FacilityCycleIdentifier(name="20XX"),
            instrument=InstrumentIdentifier(name="instrument"),
            datasets=[archive_request.dataset],
            **archive_request.investigation_identifier.model_dump(),
        )
        json_body = json.loads(archive_request.model_dump_json(exclude_none=True))
        headers = {"Authorization": f"Bearer {session_id}"}
        test_response = test_client.post(
            "/archive/idc",
            headers=headers,
            json=json_body,
        )

        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert "job_ids" in content
        assert len(content["job_ids"]) == 1
        UUID(content["job_ids"][0], version=4)

        path = "/instrument/20XX/name-visitId/type/dataset1/datafile"
        sources = [f"root://idc.ac.uk:1094/{path}"]
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

        test_response = test_client.get(f"/job/{content['job_ids'][0]}/status")
        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert "status" in content
        assert isinstance(content["status"], dict)

    def test_archive_failure(
        self,
        facility: Entity,
        investigation_type: Entity,
        dataset_type: Entity,
        facility_cycle: Entity,
        instrument: Entity,
        investigation: Entity,
        dataset_failed: Entity,
        datafile_failed: Entity,
        parameter_type_state: Entity,
        parameter_type_job_ids: Entity,
        parameter_type_deletion_date: Entity,
        parameter_type_string: Entity,
        parameter_type_numeric: Entity,
        parameter_type_date_time: Entity,
        sample_type: Entity,
        technique: Entity,
        datafile_format: Entity,
        session_id: str,
        archive_request: ArchiveRequest,
        test_client_no_archive: TestClient,
    ):
        json_body = json.loads(archive_request.model_dump_json(exclude_none=True))
        headers = {"Authorization": f"Bearer {session_id}"}
        response = test_client_no_archive.post(
            "/archive/idc",
            headers=headers,
            json=json_body,
        )

        detail = "Archive functionality not implemented for this instance"
        assert response.status_code == 501
        assert json.loads(response.content.decode())["detail"] == detail


class TestRestore:
    @pytest.mark.parametrize(
        ["restore_ids"],
        [
            pytest.param("investigation_ids"),
            pytest.param("dataset_ids"),
            pytest.param("datafile_ids"),
        ],
    )
    @pytest.mark.flaky(only_on=[ServerError], retries=3)
    def test_restore_rdc(
        self,
        submit: MagicMock,
        session_id: str,
        facility: Entity,
        investigation_type: Entity,
        facility_cycle: Entity,
        instrument: Entity,
        investigation: Entity,
        dataset_failed: Entity,
        datafile_failed: Entity,
        functional_icat_client: IcatClient,
        test_client: TestClient,
        restore_ids: str,
    ):
        if restore_ids == "investigation_ids":
            restore_request = TransferRequest(investigation_ids=[investigation.id])
        elif restore_ids == "dataset_ids":
            equals = {"investigation.id": investigation.id}
            dataset = functional_icat_client.get_single_entity("Dataset", equals)
            restore_request = TransferRequest(dataset_ids=[dataset.id])
        elif restore_ids == "datafile_ids":
            equals = {"dataset.investigation.id": investigation.id}
            datafile = functional_icat_client.get_single_entity("Datafile", equals)
            restore_request = TransferRequest(datafile_ids=[datafile.id])

        json_body = json.loads(restore_request.model_dump_json(exclude_none=True))
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
        UUID(content["job_ids"][0], version=4)

        path = "instrument/20XX/name-visitId/type/dataset/datafile"
        sources = [f"root://archive.ac.uk:1094//{path}"]
        destinations = [f"root://rdc.ac.uk:1094//{path}"]
        job = fts_job(
            sources=sources,
            destinations=destinations,
            bring_online=28800,
        )
        submit.assert_called_once_with(context=ANY, job=job)

        test_response = test_client.get(f"/job/{content['job_ids'][0]}/status")
        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert "status" in content
        assert isinstance(content["status"], dict)

    @pytest.mark.parametrize(
        ["restore_ids"],
        [
            pytest.param("investigation_ids"),
            pytest.param("dataset_ids"),
            pytest.param("datafile_ids"),
        ],
    )
    @pytest.mark.parametrize(
        ["bucket_acl"],
        [pytest.param(BucketAcl.PRIVATE), pytest.param(BucketAcl.PUBLIC_READ)],
    )
    @pytest.mark.flaky(only_on=[ServerError], retries=3)
    def test_restore_download(
        self,
        submit: MagicMock,
        session_id: str,
        facility: Entity,
        investigation_type: Entity,
        facility_cycle: Entity,
        instrument: Entity,
        investigation: Entity,
        dataset_failed: Entity,
        datafile_failed: Entity,
        mock_fts3_settings: Settings,
        functional_icat_client: IcatClient,
        test_client: TestClient,
        restore_ids: str,
        bucket_acl: BucketAcl,
        bucket_deletion: None,
    ):
        if restore_ids == "investigation_ids":
            restore_request = TransferS3Request(
                investigation_ids=[investigation.id],
                bucket_acl=bucket_acl,
            )
        elif restore_ids == "dataset_ids":
            equals = {"investigation.id": investigation.id}
            dataset = functional_icat_client.get_single_entity("Dataset", equals)
            restore_request = TransferS3Request(
                dataset_ids=[dataset.id],
                bucket_acl=bucket_acl,
            )
        elif restore_ids == "datafile_ids":
            equals = {"dataset.investigation.id": investigation.id}
            datafile = functional_icat_client.get_single_entity("Datafile", equals)
            restore_request = TransferS3Request(
                datafile_ids=[datafile.id],
                bucket_acl=bucket_acl,
            )

        json_body = json.loads(restore_request.model_dump_json(exclude_none=True))
        headers = {"Authorization": f"Bearer {session_id}"}
        test_response = test_client.post(
            "/restore/echo",
            headers=headers,
            json=json_body,
        )
        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert "job_ids" in content
        assert len(content["job_ids"]) == 1
        assert "bucket_name" in content
        if bucket_acl == BucketAcl.PUBLIC_READ:
            bucket_name = "cache-bucket"
        else:
            bucket_name = content["bucket_name"]

        s3_url = mock_fts3_settings.fts3.storage_endpoints["echo"].formatted_url
        path = "instrument/20XX/name-visitId/type/dataset/datafile"
        sources = [f"root://archive.ac.uk:1094//{path}?copy_mode=push"]
        destinations = [f"{s3_url}{bucket_name}/{path}"]
        job = fts_job(
            sources=sources,
            destinations=destinations,
            bring_online=28800,
            strict_copy=True,
        )
        submit.assert_called_once_with(context=ANY, job=job)

        test_response = test_client.get(f"/job/{content['job_ids'][0]}/status")
        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert "status" in content
        assert isinstance(content["status"], dict)


class TestTransfer:
    @pytest.mark.parametrize(
        ["restore_ids"],
        [
            pytest.param("investigation_ids"),
            pytest.param("dataset_ids"),
            pytest.param("datafile_ids"),
        ],
    )
    @pytest.mark.flaky(only_on=[ServerError], retries=3)
    def test_transfer(
        self,
        submit: MagicMock,
        session_id: str,
        facility: Entity,
        investigation_type: Entity,
        facility_cycle: Entity,
        instrument: Entity,
        investigation: Entity,
        dataset_failed: Entity,
        datafile_failed: Entity,
        mock_fts3_settings: Settings,
        functional_icat_client: IcatClient,
        test_client: TestClient,
        restore_ids: str,
    ):
        if restore_ids == "investigation_ids":
            restore_request = TransferRequest(investigation_ids=[investigation.id])
        elif restore_ids == "dataset_ids":
            equals = {"investigation.id": investigation.id}
            dataset = functional_icat_client.get_single_entity("Dataset", equals)
            restore_request = TransferRequest(dataset_ids=[dataset.id])
        elif restore_ids == "datafile_ids":
            equals = {"dataset.investigation.id": investigation.id}
            datafile = functional_icat_client.get_single_entity("Datafile", equals)
            restore_request = TransferRequest(datafile_ids=[datafile.id])

        json_body = json.loads(restore_request.model_dump_json(exclude_none=True))
        headers = {"Authorization": f"Bearer {session_id}"}
        test_response = test_client.post(
            "/transfer/echo/rdc",
            headers=headers,
            json=json_body,
        )

        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert "job_ids" in content
        assert len(content["job_ids"]) == 1
        UUID(content["job_ids"][0], version=4)

        s3_url = mock_fts3_settings.fts3.storage_endpoints["echo"].formatted_url
        path = "instrument/20XX/name-visitId/type/dataset/datafile"
        sources = [f"{s3_url}cache-bucket/{path}"]
        destinations = [f"root://rdc.ac.uk:1094//{path}"]
        job = fts_job(
            sources=sources,
            destinations=destinations,
        )
        submit.assert_called_once_with(context=ANY, job=job)

        test_response = test_client.get(f"/job/{content['job_ids'][0]}/status")
        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert "status" in content
        assert isinstance(content["status"], dict)

    def test_transfer_failure(
        self,
        session_id: str,
        mock_fts3_settings: Settings,
        test_client: TestClient,
    ):
        transfer_request = TransferRequest(datafile_ids=[1])
        json_body = json.loads(transfer_request.model_dump_json(exclude_none=True))
        headers = {"Authorization": f"Bearer {session_id}"}
        test_response = test_client.post(
            "/transfer/test/test",
            headers=headers,
            json=json_body,
        )

        assert test_response.status_code == 422
        detail = json.loads(test_response.content.decode())["detail"]
        assert "test is not a recognised storage key:" in detail


class TestDataset:
    @pytest.mark.flaky(only_on=[ServerError], retries=3)
    def test_put_dataset_retry(
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
        dataset_failed: Entity,
        datafile_failed: Entity,
        parameter_type_state: Entity,
        parameter_type_job_ids: Entity,
        parameter_type_deletion_date: Entity,
        parameter_type_string: Entity,
        parameter_type_numeric: Entity,
        parameter_type_date_time: Entity,
        sample_type: Entity,
        technique: Entity,
        datafile_format: Entity,
    ):
        get_icat_cache.cache_clear()
        headers = {"Authorization": f"Bearer {session_id}"}
        url = f"/dataset/{dataset_failed.id}/retry/idc"
        test_response = test_client.put(url, headers=headers)

        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert "dataset_ids" in content
        assert content["dataset_ids"] == [dataset_failed.id]
        assert "job_ids" in content
        assert len(content["job_ids"]) == 1
        UUID(content["job_ids"][0], version=4)

        path = "/instrument/20XX/name-visitId/type/dataset/datafile"
        sources = [f"root://idc.ac.uk:1094/{path}"]
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
            entity="Dataset",
            conditions={"name": " = 'dataset'"},
            includes=[
                "parameters.type",
                "datafiles.parameters.type",
            ],
        )
        datasets = icat_client.client.search(query=query)
        assert len(datasets) == 1
        assert len(datasets[0].parameters) == 2, datasets[0].parameters
        assert datasets[0].parameters[0].type.name == "Archival state"
        assert datasets[0].parameters[0].stringValue == "SUBMITTED"
        assert datasets[0].parameters[1].type.name == "Archival ids"
        assert datasets[0].parameters[1].stringValue is not None
        assert len(datasets[0].datafiles[0].parameters) == 1
        assert datasets[0].datafiles[0].parameters[0].type.name == "Archival state"
        assert datasets[0].datafiles[0].parameters[0].stringValue == "SUBMITTED"

    def test_put_dataset_status(
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
        dataset_failed: Entity,
        datafile_failed: Entity,
        parameter_type_state: Entity,
        parameter_type_job_ids: Entity,
        parameter_type_deletion_date: Entity,
        parameter_type_string: Entity,
        parameter_type_numeric: Entity,
        parameter_type_date_time: Entity,
        sample_type: Entity,
        technique: Entity,
        datafile_format: Entity,
    ):
        get_icat_cache.cache_clear()
        headers = {"Authorization": f"Bearer {session_id}"}
        query = "new_state=DELETED_BY_POLICY&set_deletion_date=true"
        url = f"/dataset/{dataset_failed.id}/status?{query}"
        test_response = test_client.put(url, headers=headers)

        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert content is None

        icat_client = IcatClient(session_id=session_id)
        query = Query(
            client=icat_client.client,
            entity="Dataset",
            conditions={"name": " = 'dataset'"},
            includes=[
                "parameters.type",
                "datafiles.parameters.type",
            ],
        )
        datasets = icat_client.client.search(query=query)
        assert len(datasets) == 1
        assert len(datasets[0].parameters) == 2, datasets[0].parameters
        assert datasets[0].parameters[0].type.name == "Archival state"
        assert datasets[0].parameters[0].stringValue == "DELETED_BY_POLICY"
        assert datasets[0].parameters[1].type.name == "Deletion date"
        assert datasets[0].parameters[1].dateTimeValue is not None
        assert len(datasets[0].datafiles[0].parameters) == 2
        assert datasets[0].datafiles[0].parameters[0].type.name == "Archival state"
        assert datasets[0].datafiles[0].parameters[0].stringValue == "DELETED_BY_POLICY"
        assert datasets[0].datafiles[0].parameters[1].type.name == "Deletion date"
        assert datasets[0].datafiles[0].parameters[1].dateTimeValue is not None

    def test_get_dataset_complete(
        self,
        test_client: TestClient,
        session_id: str,
        facility: Entity,
        investigation_type: Entity,
        dataset_type: Entity,
        facility_cycle: Entity,
        instrument: Entity,
        investigation: Entity,
        dataset_failed: Entity,
        datafile_failed: Entity,
        parameter_type_state: Entity,
        parameter_type_job_ids: Entity,
        parameter_type_deletion_date: Entity,
    ):
        url = f"/dataset/{dataset_failed.id}/complete"
        headers = {"Authorization": f"Bearer {session_id}"}
        test_response = test_client.get(url=url, headers=headers)

        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert content == {"complete": True}

    def test_get_dataset_percentage(
        self,
        test_client: TestClient,
        session_id: str,
        facility: Entity,
        investigation_type: Entity,
        dataset_type: Entity,
        facility_cycle: Entity,
        instrument: Entity,
        investigation: Entity,
        dataset_failed: Entity,
        datafile_failed: Entity,
        parameter_type_state: Entity,
        parameter_type_job_ids: Entity,
        parameter_type_deletion_date: Entity,
    ):
        url = f"/dataset/{dataset_failed.id}/percentage"
        headers = {"Authorization": f"Bearer {session_id}"}
        test_response = test_client.get(url=url, headers=headers)

        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert content == {"percentage_complete": 100}


class TestDatafile:
    def test_put_datafile_status(
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
        dataset_failed: Entity,
        datafile_failed: Entity,
        parameter_type_state: Entity,
        parameter_type_job_ids: Entity,
        parameter_type_deletion_date: Entity,
        parameter_type_string: Entity,
        parameter_type_numeric: Entity,
        parameter_type_date_time: Entity,
        sample_type: Entity,
        technique: Entity,
        datafile_format: Entity,
    ):
        get_icat_cache.cache_clear()
        headers = {"Authorization": f"Bearer {session_id}"}
        query = "new_state=DELETED_BY_POLICY&set_deletion_date=true"
        url = f"/datafile/{datafile_failed.id}/status?{query}"
        test_response = test_client.put(url, headers=headers)

        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert content is None

        icat_client = IcatClient(session_id=session_id)
        query = Query(
            client=icat_client.client,
            entity="Dataset",
            conditions={"name": " = 'dataset'"},
            includes=[
                "parameters.type",
                "datafiles.parameters.type",
            ],
        )
        datasets = icat_client.client.search(query=query)
        assert len(datasets) == 1
        assert len(datasets[0].parameters) == 1, datasets[0].parameters
        assert datasets[0].parameters[0].type.name == "Archival state"
        assert datasets[0].parameters[0].stringValue == "FAILED"
        assert len(datasets[0].datafiles[0].parameters) == 2
        assert datasets[0].datafiles[0].parameters[0].type.name == "Archival state"
        assert datasets[0].datafiles[0].parameters[0].stringValue == "DELETED_BY_POLICY"
        assert datasets[0].datafiles[0].parameters[1].type.name == "Deletion date"
        assert datasets[0].datafiles[0].parameters[1].dateTimeValue is not None


class TestCancel:
    def test_cancel(
        self,
        test_client: TestClient,
        mocker: MockerFixture,
    ):
        fts_submit_mock = mocker.patch("datastore_api.clients.fts3_client.fts3.cancel")
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


class TestBucket:
    def test_get_bucket_data_private(
        self,
        mock_fts3_settings: Settings,
        test_client: TestClient,
        bucket_name_private: str,
    ):
        s3_url = mock_fts3_settings.fts3.storage_endpoints["echo"].url
        headers = {"Authorization": f"Bearer {SESSION_ID}"}
        url = f"/bucket/echo/{bucket_name_private}"
        test_response = test_client.get(url, headers=headers)
        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert len(content) == 1
        assert "test" in content
        assert content["test"].startswith(f"{s3_url}{bucket_name_private}/test?")

    def test_download_status(
        self,
        test_client: TestClient,
        cache_bucket: str,
        bucket_name_private: str,
    ):
        headers = {"Authorization": f"Bearer {SESSION_ID}"}
        url = f"/bucket/echo/{bucket_name_private}/status"
        test_response = test_client.get(url=url, headers=headers)

        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert content == {"status": STATUSES}

    def test_download_complete(self, test_client: TestClient, bucket_name_private: str):
        headers = {"Authorization": f"Bearer {SESSION_ID}"}
        url = f"/bucket/echo/{bucket_name_private}/complete"
        test_response = test_client.get(url=url, headers=headers)

        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert content == {"complete": True}

    def test_download_percentage(
        self,
        test_client: TestClient,
        cache_bucket: str,
        bucket_name_private: str,
    ):
        headers = {"Authorization": f"Bearer {SESSION_ID}"}
        url = f"/bucket/echo/{bucket_name_private}/percentage"
        test_response = test_client.get(url=url, headers=headers)

        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert content == {"percentage_complete": 100.0}

    def test_delete_bucket(self, test_client: TestClient, bucket_name_private: str):
        headers = {"Authorization": f"Bearer {SESSION_ID}"}
        url = f"/bucket/echo/{bucket_name_private}"
        test_response = test_client.delete(url=url, headers=headers)

        assert test_response.status_code == 200

    def test_bucket_failure(
        self,
        mock_fts3_settings: Settings,
        test_client: TestClient,
    ):
        test_response = test_client.get("/bucket/idc/test/complete")

        assert test_response.status_code == 422
        detail = json.loads(test_response.content.decode())["detail"]
        assert "idc is disk, not S3 storage" == detail


class TestSize:
    @pytest.mark.parametrize(
        ["restore_ids"],
        [
            pytest.param("investigation_ids"),
            pytest.param("dataset_ids"),
            pytest.param("datafile_ids"),
        ],
    )
    def test_size(
        self,
        submit: MagicMock,
        session_id: str,
        facility: Entity,
        investigation_type: Entity,
        facility_cycle: Entity,
        instrument: Entity,
        investigation: Entity,
        dataset_failed: Entity,
        datafile_failed: Entity,
        mock_fts3_settings: Settings,
        functional_icat_client: IcatClient,
        test_client: TestClient,
        restore_ids: str,
    ):
        if restore_ids == "investigation_ids":
            restore_request = TransferRequest(investigation_ids=[investigation.id])
        elif restore_ids == "dataset_ids":
            equals = {"investigation.id": investigation.id}
            dataset = functional_icat_client.get_single_entity("Dataset", equals)
            restore_request = TransferRequest(dataset_ids=[dataset.id])
        elif restore_ids == "datafile_ids":
            equals = {"dataset.investigation.id": investigation.id}
            datafile = functional_icat_client.get_single_entity("Datafile", equals)
            restore_request = TransferRequest(datafile_ids=[datafile.id])

        json_body = json.loads(restore_request.model_dump_json(exclude_none=True))
        headers = {"Authorization": f"Bearer {session_id}"}
        test_response = test_client.post(
            "/size",
            headers=headers,
            json=json_body,
        )

        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert content == 1000
