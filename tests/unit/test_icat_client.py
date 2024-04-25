from fastapi import HTTPException
import pytest
from pytest_mock import MockerFixture

from datastore_api.icat_client import IcatClient
from datastore_api.models.login import Credentials, LoginRequest
from fixtures import icat_client, icat_client_empty_search, SESSION_ID


INSUFFICIENT_PERMISSIONS = (
    "fastapi.exceptions.HTTPException: 403: insufficient permissions"
)


class TestIcatClient:
    def test_validate_entities(self):
        with pytest.raises(HTTPException) as e:
            IcatClient._validate_entities([], [1])

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

    @pytest.mark.parametrize(
        ["investigation_ids", "dataset_ids", "datafile_ids", "expected_paths"],
        [
            pytest.param([], [], [], set(), id="No ids"),
            pytest.param(
                [1],
                [1],
                [1],
                {"instrument/20XX/name-visitId/type/dataset/datafile"},
                id="All ids",
            ),
        ],
    )
    def test_get_paths(
        self,
        icat_client: IcatClient,
        mocker: MockerFixture,
        investigation_ids: list[int],
        dataset_ids: list[int],
        datafile_ids: list[int],
        expected_paths: set[str],
    ):
        investigation_instrument = mocker.MagicMock(name="investigation_instrument")
        investigation_instrument.instrument.name = "instrument"
        investigation_cycle = mocker.MagicMock(name="investigation_cycle")
        investigation_cycle.facilityCycle.name = "20XX"
        investigation = mocker.MagicMock(name="investigation")
        investigation.investigationInstruments = [investigation_instrument]
        investigation.investigationFacilityCycles = [investigation_cycle]
        investigation.name = "name"
        investigation.visitId = "visitId"

        dataset = mocker.MagicMock(name="dataset")
        dataset.name = "dataset"
        dataset.type.name = "type"
        dataset.investigation = investigation

        datafile = mocker.MagicMock(name="datafile")
        datafile.name = "datafile"
        datafile.dataset = dataset

        dataset.datafiles = [datafile]
        investigation.datasets = [dataset]

        icat_client.client.search.side_effect = [[investigation], [dataset], [datafile]]
        paths = icat_client.get_paths(
            session_id=SESSION_ID,
            investigation_ids=investigation_ids,
            dataset_ids=dataset_ids,
            datafile_ids=datafile_ids,
        )

        assert paths == expected_paths
        assert icat_client.client.sessionId is None

    def test_get_single_entity_failure(self, icat_client_empty_search: IcatClient):
        with pytest.raises(HTTPException) as e:
            icat_client_empty_search.get_single_entity(
                session_id=SESSION_ID,
                entity="Facility",
                conditions={"name": "facility"},
            )

        err = (
            "fastapi.exceptions.HTTPException: 400: "
            "No Facility with conditions {'name': 'facility'}"
        )
        assert e.exconly() == err

    def test_create_many(self, icat_client: IcatClient):
        icat_client.create_many(session_id=SESSION_ID, beans=[])
        icat_client.client.createMany.assert_called_once_with(beans=[])
