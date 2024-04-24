from datetime import datetime

from icat import ICATSessionError
import pytest
from pytest_mock import mocker, MockerFixture

from datastore_api.config import IcatSettings
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


@pytest.fixture(scope="session")
def investigation():
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
