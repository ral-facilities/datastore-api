from datetime import datetime

from icat import ICATSessionError
from pydantic import ValidationError
import pytest
from pytest_mock import mocker, MockerFixture

from datastore_api.config import (
    Fts3Settings,
    FunctionalUser,
    get_settings,
    IcatSettings,
    Settings,
)
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


SESSION_ID = "00000000-0000-0000-0000-000000000000"


@pytest.fixture(scope="function")
def investigation(mocker: MockerFixture):
    try:
        settings = get_settings()
    except ValidationError:
        # Assume the issue is that we do not have the cert to communicate with FTS.
        # This will be the case for GHA workflows, in which case,
        # pass a readable file to satisfy the validator and mock requests to FTS.
        fts3_settings = Fts3Settings(
            endpoint="",
            instrument_data_cache="",
            user_data_cache="",
            tape_archive="",
            x509_user_cert=__file__,
            x509_user_key=__file__,
        )
        settings = Settings(fts3=fts3_settings)

        get_settings_mock = mocker.patch("datastore_api.models.archive.get_settings")
        get_settings_mock.return_value = settings

    dataset = Dataset(
        name="dataset",
        datasetType=DatasetType(name="type"),
        datafiles=[Datafile(name="datafile")],
    )
    return Investigation(
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


def login_side_effect(auth: str, credentials: dict) -> str:
    if auth == "simple":
        return SESSION_ID

    raise ICATSessionError("test")


@pytest.fixture(scope="session")
def icat_settings():
    functional_user = FunctionalUser(auth="simple", username="root", password="pw")
    return IcatSettings(url="", functional_user=functional_user)


@pytest.fixture(scope="function")
def icat_client(icat_settings: IcatSettings, mocker: MockerFixture):
    client = mocker.patch("datastore_api.icat_client.Client")
    client.return_value.login.side_effect = login_side_effect
    client.return_value.getUserName.return_value = "simple/root"
    client.return_value.search.return_value = [mocker.MagicMock()]

    mocker.patch("datastore_api.icat_client.Query")

    return IcatClient(icat_settings=icat_settings)


@pytest.fixture(scope="function")
def icat_client_empty_search(icat_settings: IcatSettings, mocker: MockerFixture):
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

    return IcatClient(icat_settings=icat_settings)
