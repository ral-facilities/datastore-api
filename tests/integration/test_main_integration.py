from datetime import datetime
import json
import logging
from typing import Generator
from unittest.mock import ANY, MagicMock

from fastapi.testclient import TestClient
from icat import ICATObjectExistsError
from icat.entity import Entity
from icat.query import Query
import pytest
from pytest_mock import MockerFixture

from datastore_api.icat_client import IcatClient
from datastore_api.main import app, get_icat_client
from datastore_api.models.archive import (
    ArchiveRequest,
    Facility,
    FacilityCycle,
    Instrument,
    Investigation,
    InvestigationType,
)
from datastore_api.models.restore import RestoreRequest


log = logging.getLogger("tests")


@pytest.fixture(scope="function")
def test_client(mocker: MockerFixture) -> TestClient:
    # TODO remove this once we have a working FTS container for the tests
    mocker.patch("datastore_api.main.fts3.Context")

    fts_submit_mock = mocker.patch("datastore_api.main.fts3.submit")
    fts_submit_mock.return_value = "0"

    fts_status_mock = mocker.patch("datastore_api.main.fts3.get_job_status")
    fts_status_mock.return_value = {"key": "value"}

    fts_submit_mock = mocker.patch("datastore_api.main.fts3.cancel")
    fts_submit_mock.return_value = "CANCELED"
    return TestClient(app)


@pytest.fixture(scope="function")
def submit(mocker: MockerFixture) -> MagicMock:
    # TODO remove this once we have a working FTS container for the tests
    fts_submit_mock = mocker.patch("datastore_api.main.fts3.submit")
    fts_submit_mock.return_value = "0"
    return fts_submit_mock


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
        icat_client.client.deleteMany(investigations)
    finally:
        icat_client.client.sessionId = None


@pytest.fixture(scope="function")
def facility(session_id: str) -> Generator[Entity, None, None]:
    facility = create(session_id=session_id, entity="Facility", name="facility")

    yield facility

    delete(session_id=session_id, entity=facility)


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
def investigation(
    session_id: str,
    facility: Entity,
    investigation_type: Entity,
    instrument: Entity,
    facility_cycle: Entity,
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
    investigation = create(
        session_id=session_id,
        entity="Investigation",
        name="name",
        visitId="visitId",
        title="title",
        facility=facility,
        type=investigation_type,
        investigationInstruments=[investigation_instrument],
        investigationFacilityCycles=[investigation_facility_cycle],
    )

    yield investigation

    delete(session_id=session_id, entity=investigation)


def create(session_id: str, entity: str, **kwargs) -> Entity:
    icat_client = get_icat_client()
    try:
        icat_client.client.sessionId = session_id
        icat_entity = icat_client.client.new(obj=entity, **kwargs)
        icat_entity_id = icat_client.client.create(icat_entity)
        icat_entity.id = icat_entity_id

    except ICATObjectExistsError as e:
        log.warning(str(e))
        icat_entity = icat_client.get_single_entity(
            entity=entity,
            name=kwargs["name"],
            facility_name=kwargs["facility"].name if "facility" in kwargs else None,
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
            "verify_checksum": False,
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


class TestMainIntegration:
    def test_archive(
        self,
        test_client: TestClient,
        submit: MagicMock,
        session_id: str,
        facility: Entity,
        investigation_type: Entity,
        facility_cycle: Entity,
        instrument: Entity,
    ):
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
            facilityCycle=FacilityCycle(name="20XX"),
        )
        archive_request = ArchiveRequest(investigations=[investigation])
        json_body = json.loads(archive_request.json())
        headers = {"Authorization": f"Bearer {session_id}"}
        test_response = test_client.post("/archive", headers=headers, json=json_body)

        content = json.loads(test_response.content)
        assert test_response.status_code == 200, content
        assert content == {"job_id": "0"}

        sources = [
            "cephfs://idc//instrument/20XX/name-visitId",
            "cephfs://udc//instrument/20XX/name-visitId",
        ]
        destinations = ["tape://archive//instrument/20XX/name-visitId"]
        job = fts_job(sources=sources, destinations=destinations)
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
                ],
            )
            investigations = icat_client.client.search(query=query)
            assert len(investigations) == 1
            investigation = investigations[0]

            investigation_instruments = investigation.investigationInstruments
            assert len(investigation_instruments) == 1

            investigation_facility_cycles = investigation.investigationFacilityCycles
            assert len(investigation_facility_cycles) == 1

            assert investigation.name == "name"
            assert investigation.visitId == "visitId"
            assert investigation.title == "title"
            assert investigation.summary == "summary"
            assert investigation.startDate is not None
            assert investigation.endDate is not None
            assert investigation.startDate is not None
            assert investigation.facility.name == "facility"
            assert investigation.type.name == "type"
            assert investigation_instruments[0].instrument.name == "instrument"
            assert investigation_facility_cycles[0].facilityCycle.name == "20XX"
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
        assert content == {"job_id": "0"}

        sources = ["tape://archive//instrument/20XX/name-visitId"]
        destinations = ["cephfs://udc//instrument/20XX/name-visitId"]
        job = fts_job(
            sources=sources,
            destinations=destinations,
            bring_online=28800,
            copy_pin_lifetime=28800,
        )
        submit.assert_called_once_with(context=ANY, job=job)
