from fastapi import HTTPException
from icat.entity import Entity
import pytest
from pytest_mock import MockerFixture

from datastore_api.clients.icat_client import IcatCache, IcatClient
from datastore_api.config import IcatSettings, Settings
from datastore_api.models.icat import Sample
from datastore_api.models.login import Credentials, LoginRequest
from tests.fixtures import (
    archive_request_parameters,
    archive_request_sample,
    dataset_type,
    facility,
    facility_cycle,
    functional_icat_client,
    icat_client,
    icat_client_empty_search,
    icat_settings,
    instrument,
    investigation,
    investigation_type,
    mock_fts3_settings,
    parameter_type_date_time,
    parameter_type_numeric,
    parameter_type_string,
    sample_type,
    SESSION_ID,
    submit,
)


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
        assert icat_client.client.sessionId == SESSION_ID

    def test_login_failure(self, icat_client: IcatClient):
        credentials = Credentials(username="root", password="pw")
        login_request = LoginRequest(auth="simpl", credentials=credentials)
        with pytest.raises(HTTPException) as e:
            icat_client.login(login_request=login_request)

        assert e.exconly() == "fastapi.exceptions.HTTPException: 401: test"
        assert icat_client.client.sessionId is None

    def test_functional_login(self, icat_client: IcatClient):
        credentials = {"username": "root", "password": "pw"}

        session_id = icat_client.login_functional()

        assert session_id == SESSION_ID
        assert icat_client.client.sessionId == SESSION_ID
        icat_client.client.login.assert_called_once_with("simple", credentials)

    def test_authorise_admin_failure(self, icat_client: IcatClient):
        icat_client.settings.admin_users = []
        with pytest.raises(HTTPException) as e:
            icat_client.authorise_admin()

        assert e.exconly() == INSUFFICIENT_PERMISSIONS

    @pytest.mark.parametrize(
        ["investigation_ids", "dataset_ids", "datafile_ids", "expected"],
        [
            pytest.param([], [], [], 0, id="No ids"),
            pytest.param([], [], [1], 1, id="Datafile ids"),
            pytest.param([], [1], [1], 1, id="Dataset ids"),
            pytest.param([1], [1], [1], 1, id="Investigation ids"),
        ],
    )
    def test_get_unique_datafiles(
        self,
        icat_client: IcatClient,
        mocker: MockerFixture,
        investigation_ids: list[int],
        dataset_ids: list[int],
        datafile_ids: list[int],
        expected: int,
    ):
        investigation = mocker.MagicMock(name="investigation")
        investigation.id = 1

        dataset = mocker.MagicMock(name="dataset")
        dataset.id = 1
        dataset.investigation = investigation

        datafile = mocker.MagicMock(name="datafile")
        datafile.id = 1
        datafile.dataset = dataset

        dataset.datafiles = [datafile]
        investigation.datasets = [dataset]

        icat_client.client.search.side_effect = [[investigation], [dataset], [datafile]]
        datafiles = icat_client.get_unique_datafiles(
            investigation_ids=investigation_ids,
            dataset_ids=dataset_ids,
            datafile_ids=datafile_ids,
        )

        assert len(datafiles) == expected
        assert icat_client.client.sessionId is None

    def test_get_single_entity_failure(self, icat_client_empty_search: IcatClient):
        with pytest.raises(HTTPException) as e:
            icat_client_empty_search.get_single_entity(
                entity="Facility",
                equals={"name": "facility"},
            )

        err = (
            "fastapi.exceptions.HTTPException: 400: No Facility with "
            "{'name': 'facility'} and fields containing None"
        )
        assert e.exconly() == err

    def test_create_many(self, icat_client: IcatClient):
        icat_client.create_many(beans=[])
        icat_client.client.createMany.assert_called_once_with(beans=[])

    def test_check_job_id(self, icat_client: IcatClient):
        with pytest.raises(HTTPException) as e:
            icat_client.check_job_id(job_id="0")

        err = "fastapi.exceptions.HTTPException: 400: Archival jobs cannot be cancelled"
        assert e.exconly() == err

    def test_extract_sample(
        self,
        functional_icat_client: IcatClient,
        investigation: Entity,
        sample_type: Entity,
        parameter_type_date_time: Entity,
        parameter_type_numeric: Entity,
        parameter_type_string: Entity,
        archive_request_sample: Sample,
    ):
        sample_entity = functional_icat_client._extract_sample(
            investigation,
            archive_request_sample,
        )
        new_sample_id = sample_entity.id

        sample_entity = functional_icat_client._extract_sample(
            investigation,
            archive_request_sample,
        )
        existing_sample_id = sample_entity.id

        assert new_sample_id == existing_sample_id


class TestIcatCache:
    def test_icat_cache(
        self,
        facility: Entity,
        icat_settings: IcatSettings,
        mocker: MockerFixture,
    ):
        module = "datastore_api.clients.icat_client.get_settings"
        get_settings_mock = mocker.patch(module)
        model_dump = icat_settings.model_dump(exclude="create_parameter_types")
        icat_settings_copy = IcatSettings(create_parameter_types=True, **model_dump)
        get_settings_mock.return_value = Settings(icat=icat_settings_copy)

        icat_cache = IcatCache()

        assert icat_cache.parameter_type_job_state is not None
        assert icat_cache.parameter_type_job_ids is not None
        assert icat_cache.parameter_type_deletion_date is not None
