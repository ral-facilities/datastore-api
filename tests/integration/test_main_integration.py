from datetime import datetime
import json
import logging
from typing import Generator
from unittest.mock import ANY, MagicMock

from fastapi.testclient import TestClient
import fts3.rest.client.easy as fts3
from icat import ICATObjectExistsError
from icat.entity import Entity
from icat.query import Query
from pydantic import UUID4, ValidationError
import pytest
from pytest_mock import MockerFixture

from datastore_api.config import Fts3Settings, get_settings, Settings
from datastore_api.icat_client import IcatClient
from datastore_api.main import app, get_icat_client
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
from tests.unit.fixtures import SESSION_ID


log = logging.getLogger("tests")


@pytest.fixture(scope="function")
def test_client(mocker: MockerFixture) -> TestClient:
    try:
        settings = get_settings()
    except ValidationError:
        # Assume the issue is that we do not have the cert to communicate with FTS.
        # This will be the case for GHA workflows, in which case,
        # pass a readable file to satisfy the validator and mock requests to FTS.
        fts3_settings = Fts3Settings(
            endpoint="https://fts-test01.gridpp.rl.ac.uk:8446",
            instrument_data_cache="root://idc:1094/",
            user_data_cache="root://udc:1094/",
            tape_archive="root://archive:1094/",
            x509_user_cert=__file__,
            x509_user_key=__file__,
        )
        settings = Settings(fts3=fts3_settings)
        get_settings_mock = mocker.patch("datastore_api.main.get_settings")
        get_settings_mock.return_value = settings

        get_settings_investigation_mock = mocker.patch(
            "datastore_api.models.archive.get_settings",
        )
        get_settings_investigation_mock.return_value = settings

        mocker.patch("datastore_api.fts3_client.fts3.Context")

        fts_submit_mock = mocker.patch("datastore_api.fts3_client.fts3.submit")
        fts_submit_mock.return_value = SESSION_ID

        fts_status_mock = mocker.patch("datastore_api.fts3_client.fts3.get_job_status")
        fts_status_mock.return_value = {"key": "value"}

        fts_cancel_mock = mocker.patch("datastore_api.fts3_client.fts3.cancel")
        fts_cancel_mock.return_value = "CANCELED"

    return TestClient(app)


@pytest.fixture(scope="function")
def submit(mocker: MockerFixture) -> MagicMock:
    submit_mock = MagicMock(wraps=fts3.submit)
    mocker.patch("datastore_api.fts3_client.fts3.submit", submit_mock)
    return submit_mock


@pytest.fixture(scope="function")
def session_id(test_client: TestClient) -> Generator[str, None, None]:
    credentials = {"username": "root", "password": "pw"}
    login_request = {"auth": "simple", "credentials": credentials}
    response = test_client.post("/login", content=json.dumps(login_request))

    session_id = json.loads(response.content)["sessionId"]

    yield session_id

    try:
        icat_client = get_icat_client()
        icat_client.client.sessionId = session_id

        query = Query(
            client=icat_client.client,
            entity="Investigation",
            conditions={"name": " = 'name'"},
        )
        investigations = icat_client.client.search(query)

        query = Query(
            client=icat_client.client,
            entity="Dataset",
            conditions={"name": " = 'dataset'"},
        )
        datasets = icat_client.client.search(query)

        query = Query(
            client=icat_client.client,
            entity="Datafile",
            conditions={"name": " = 'datafile'"},
        )
        datafiles = icat_client.client.search(query)

        icat_client.client.deleteMany(investigations + datasets + datafiles)
    finally:
        icat_client.client.sessionId = None


@pytest.fixture(scope="function")
def facility(session_id: str) -> Generator[Entity, None, None]:
    facility = create(session_id=session_id, entity="Facility", name="facility")

    yield facility

    delete(session_id=session_id, entity=facility)


@pytest.fixture(scope="function")
def dataset_type(
    session_id: str,
    facility: Entity,
) -> Generator[Entity, None, None]:
    dataset_type = create(
        session_id=session_id,
        entity="DatasetType",
        name="type",
        facility=facility,
    )

    yield dataset_type

    delete(session_id=session_id, entity=dataset_type)


@pytest.fixture(scope="function")
def investigation_type(
    session_id: str,
    facility: Entity,
) -> Generator[Entity, None, None]:
    investigation_type = create(
        session_id=session_id,
        entity="InvestigationType",
        name="type",
        facility=facility,
    )

    yield investigation_type

    delete(session_id=session_id, entity=investigation_type)


@pytest.fixture(scope="function")
def facility_cycle(
    session_id: str,
    facility: Entity,
) -> Generator[Entity, None, None]:
    facility_cycle = create(
        session_id=session_id,
        entity="FacilityCycle",
        name="20XX",
        facility=facility,
    )

    yield facility_cycle

    delete(session_id=session_id, entity=facility_cycle)


@pytest.fixture(scope="function")
def instrument(
    session_id: str,
    facility: Entity,
) -> Generator[Entity, None, None]:
    instrument = create(
        session_id=session_id,
        entity="Instrument",
        name="instrument",
        facility=facility,
    )

    yield instrument

    delete(session_id=session_id, entity=instrument)


@pytest.fixture(scope="function")
def parameter_type_state(
    session_id: str,
    facility: Entity,
) -> Generator[Entity, None, None]:
    parameter_type = create(
        session_id=session_id,
        entity="ParameterType",
        name="Archival state",
        facility=facility,
        units="",
        valueType="STRING",
        applicableToDataset=True,
        applicableToDatafile=True,
    )

    yield parameter_type

    delete(session_id=session_id, entity=parameter_type)


@pytest.fixture(scope="function")
def parameter_type_job_ids(
    session_id: str,
    facility: Entity,
) -> Generator[Entity, None, None]:
    parameter_type = create(
        session_id=session_id,
        entity="ParameterType",
        name="Archival ids",
        facility=facility,
        units="",
        valueType="STRING",
        applicableToDataset=True,
    )

    yield parameter_type

    delete(session_id=session_id, entity=parameter_type)


@pytest.fixture(scope="function")
def investigation(
    session_id: str,
    facility: Entity,
    investigation_type: Entity,
    instrument: Entity,
    facility_cycle: Entity,
    dataset_type: Entity,
) -> Generator[Entity, None, None]:
    icat_client = get_icat_client()
    investigation_instrument = icat_client.client.new(
        obj="InvestigationInstrument",
        instrument=instrument,
    )
    investigation_facility_cycle = icat_client.client.new(
        obj="InvestigationFacilityCycle",
        facilityCycle=facility_cycle,
    )
    datafile = icat_client.client.new(
        obj="Datafile",
        name="datafile",
    )
    dataset = icat_client.client.new(
        obj="Dataset",
        name="dataset",
        type=dataset_type,
        datafiles=[datafile],
    )
    investigation = create(
        session_id=session_id,
        entity="Investigation",
        name="name",
        visitId="visitId",
        title="title",
        summary="summary",
        startDate=datetime.now(),
        endDate=datetime.now(),
        releaseDate=datetime.now(),
        facility=facility,
        type=investigation_type,
        investigationInstruments=[investigation_instrument],
        investigationFacilityCycles=[investigation_facility_cycle],
        datasets=[dataset],
    )

    yield investigation

    delete(session_id=session_id, entity=investigation)


@pytest.fixture(scope="function")
def investigation_tear_down(
    session_id: str,
) -> Generator[None, None, None]:
    yield None

    icat_client = get_icat_client()
    investigation = icat_client.get_single_entity(
        session_id=session_id,
        entity="Investigation",
        conditions=IcatClient.build_conditions({"name": "name", "visitId": "visitId"}),
        allow_empty=True,
    )
    if investigation is not None:
        delete(session_id=session_id, entity=investigation)


@pytest.fixture(scope="function")
def dataset_with_job_id(
    session_id: str,
    dataset_type: Entity,
    parameter_type_job_ids: Entity,
    investigation: Entity,
) -> Generator[Entity, None, None]:
    icat_client = get_icat_client()
    parameter = icat_client.client.new(
        obj="DatasetParameter",
        stringValue="0,1,2",
        type=parameter_type_job_ids,
    )
    dataset = create(
        session_id=session_id,
        entity="Dataset",
        name="dataset1",
        type=dataset_type,
        investigation=investigation,
        parameters=[parameter],
    )

    yield dataset

    delete(session_id=session_id, entity=dataset)


def create(session_id: str, entity: str, **kwargs) -> Entity:
    icat_client = get_icat_client()
    try:
        icat_client.client.sessionId = session_id
        icat_entity = icat_client.client.new(obj=entity, **kwargs)
        icat_entity_id = icat_client.client.create(icat_entity)
        icat_entity.id = icat_entity_id

    except ICATObjectExistsError as e:
        log.warning(str(e))
        conditions = {"name": kwargs["name"]}
        if "facility" in kwargs:
            conditions["facility.name"] = kwargs["facility"].name

        icat_entity = icat_client._get_single_entity(
            entity=entity,
            conditions=IcatClient.build_conditions(conditions),
        )
    finally:
        icat_client.client.sessionId = None

    return icat_entity


def delete(session_id: str, entity: Entity) -> None:
    icat_client = get_icat_client()
    try:
        icat_client.client.sessionId = session_id
        icat_client.client.delete(entity)
    finally:
        icat_client.client.sessionId = None


def fts_job(
    sources: list[str],
    destinations: list[str],
    bring_online: int = None,
    copy_pin_lifetime: int = None,
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
            "archive_timeout": None,
            "copy_pin_lifetime": copy_pin_lifetime,
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
            bring_online=None,
            copy_pin_lifetime=None,
        )
        submit.assert_called_once_with(context=ANY, job=job)

        try:
            icat_client = get_icat_client()
            icat_client.client.sessionId = session_id
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
        finally:
            icat_client.client.sessionId = None

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
            bring_online=None,
            copy_pin_lifetime=None,
        )
        submit.assert_called_once_with(context=ANY, job=job)

        try:
            icat_client = get_icat_client()
            icat_client.client.sessionId = session_id
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
        finally:
            icat_client.client.sessionId = None


class TestRestore:
    def test_restore(
        self,
        test_client: TestClient,
        submit: MagicMock,
        session_id: str,
        facility: Entity,
        investigation_type: Entity,
        facility_cycle: Entity,
        instrument: Entity,
        investigation: Entity,
    ):
        restore_request = RestoreRequest(investigation_ids=[investigation.id])
        json_body = json.loads(restore_request.json())
        headers = {"Authorization": f"Bearer {session_id}"}
        test_response = test_client.post("/restore", headers=headers, json=json_body)

        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert "job_ids" in content
        assert len(content["job_ids"]) == 1
        UUID4(content["job_ids"][0])

        path = "/instrument/20XX/name-visitId/type/dataset/datafile"
        sources = [f"root://archive:1094/{path}"]
        destinations = [f"root://udc:1094/{path}"]
        job = fts_job(
            sources=sources,
            destinations=destinations,
            bring_online=28800,
            copy_pin_lifetime=28800,
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
