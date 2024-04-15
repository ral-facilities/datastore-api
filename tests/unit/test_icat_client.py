from datetime import datetime

from fastapi import HTTPException
from icat import ICATSessionError
import pytest
from pytest_mock import mocker, MockerFixture

from datastore_api.config import IcatSettings, IcatUser
from datastore_api.icat_client import IcatClient
from datastore_api.models.archive import (
    Datafile,
    Dataset,
    DatasetType,
    Facility,
    FacilityCycle,
    Instrument,
    Investigation,
    InvestigationType,
)
from datastore_api.models.login import Credentials, LoginRequest


SESSION_ID = "00000000-0000-0000-0000-000000000000"
INSUFFICIENT_PERMISSIONS = (
    "fastapi.exceptions.HTTPException: 403: insufficient permissions"
)


def login_side_effect(auth: str, credentials: dict) -> str:
    if auth == "simple":
        return SESSION_ID

    raise ICATSessionError("test")


@pytest.fixture(scope="function")
def icat_client(mocker: MockerFixture):
    client = mocker.patch("datastore_api.icat_client.Client")
    client.return_value.login.side_effect = login_side_effect
    client.return_value.getUserName.return_value = "simple/root"
    client.return_value.search.return_value = [mocker.MagicMock()]

    mocker.patch("datastore_api.icat_client.Query")

    return IcatClient(icat_settings=IcatSettings(url=""))


@pytest.fixture(scope="function")
def icat_client_empty_search(mocker: MockerFixture):
    def generator():
        yield []
        while True:
            yield [mocker.MagicMock()]

    iterator = generator()

    def search_side_effect(**kwargs):
        return next(iterator)

    client = mocker.patch("datastore_api.icat_client.Client")
    client.return_value.login.side_effect = login_side_effect
    client.return_value.getUserName.return_value = "simple/root"
    client.return_value.search.side_effect = search_side_effect

    mocker.patch("datastore_api.icat_client.Query")

    return IcatClient(icat_settings=IcatSettings(url=""))


class TestIcatClient:
    def test_build_path(self):
        assert IcatClient.build_path("a", "b", "c", "d") == "/a/b/c-d"

    def test_validate_entities(self):
        with pytest.raises(HTTPException) as e:
            IcatClient.validate_entities([], [1])

        assert e.exconly() == INSUFFICIENT_PERMISSIONS

    def test_login_success(self, icat_client: IcatClient):
        credentials = Credentials(username="root", password="pw")
        login_request = LoginRequest(auth="simple", credentials=credentials)
        session_id = icat_client.login(login_request=login_request)
        assert session_id == SESSION_ID
        assert icat_client.client.sessionId is None

    def test_login_failure(self, icat_client: IcatClient):
        credentials = Credentials(username="root", password="pw")
        login_request = LoginRequest(auth="simpl", credentials=credentials)
        with pytest.raises(HTTPException) as e:
            icat_client.login(login_request=login_request)

        assert e.exconly() == "fastapi.exceptions.HTTPException: 401: test"
        assert icat_client.client.sessionId is None

    def test_authorise_admin_failure(self, icat_client: IcatClient):
        with pytest.raises(HTTPException) as e:
            icat_client.authorise_admin(session_id=SESSION_ID)

        assert e.exconly() == INSUFFICIENT_PERMISSIONS
        assert icat_client.client.sessionId is None

    def test_create_entities(self, icat_client_empty_search: IcatClient):
        self._test_create_entities(icat_client_empty_search)

    def test_create_entities_existing_investigation(self, icat_client: IcatClient):
        self._test_create_entities(icat_client)

    def _test_create_entities(self, icat_client: IcatClient):
        dataset = Dataset(
            name="dataset",
            datasetType=DatasetType(name="type"),
            datafiles=[Datafile(name="datafile")],
        )
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
            datasets=[dataset],
        )

        paths = icat_client.create_entities(
            session_id=SESSION_ID,
            investigations=[investigation],
        )
        assert paths == {"/instrument/20XX/name-visitId"}
        assert icat_client.client.sessionId is None

    def test_get_investigation_paths(self, icat_client: IcatClient):
        paths = icat_client.get_investigation_paths(
            session_id=SESSION_ID,
            investigation_ids=[1],
        )

        # Don't assert the path as the Mocked object does not have meaningful attributes
        assert len(paths) == 1
        assert icat_client.client.sessionId is None

    def test_get_single_entity_failure(self, icat_client_empty_search: IcatClient):
        with pytest.raises(HTTPException) as e:
            icat_client_empty_search.get_single_entity("Facility", {"name": "facility"})

        err = (
            "fastapi.exceptions.HTTPException: 400: "
            "No Facility with conditions {'name': 'facility'}"
        )
        assert e.exconly() == err
