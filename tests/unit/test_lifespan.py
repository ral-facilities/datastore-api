from unittest.mock import call
from urllib.error import URLError

from icat.entity import Entity
import pytest
from pytest_mock import MockerFixture

from datastore_api.config import Settings
from datastore_api.icat_client import IcatClient
from datastore_api.lifespan import (
    lifespan,
    LOGGER,
    poll_fts,
    StateCounter,
    update_jobs,
)
from tests.fixtures import (
    dataset_type,
    dataset_with_job_id,
    facility,
    facility_cycle,
    functional_icat_client,
    instrument,
    investigation,
    investigation_type,
    mock_fts3_settings,
    parameter_type_job_ids,
    parameter_type_state,
    submit,
)


class TestLifespan:
    async def test_lifespan(self, mock_fts3_settings: Settings):
        context_manager = lifespan(None)
        generator = context_manager.func(*context_manager.args)

        assert await generator.__anext__() is None

        with pytest.raises(StopAsyncIteration):
            await generator.__anext__()

    def test_poll_fts_success(
        self,
        mock_fts3_settings: Settings,
        mocker: MockerFixture,
    ):
        icat_client = IcatClient()
        icat_client.login_functional()
        delete_many = mocker.patch.object(
            icat_client.client,
            "deleteMany",
            wraps=icat_client.client.deleteMany,
        )

        poll_fts(icat_client)

        delete_many.assert_called_once_with([])

    def test_poll_fts_failure(
        self,
        mock_fts3_settings: Settings,
        mocker: MockerFixture,
    ):
        icat_client = IcatClient()
        icat_client.login_functional()
        delete_many = mocker.patch.object(
            icat_client.client,
            "deleteMany",
            wraps=icat_client.client.deleteMany,
        )

        error = mocker.patch.object(LOGGER, "error", wraps=LOGGER.error)

        update_jobs_mock = mocker.MagicMock()
        update_jobs_mock.side_effect = URLError("test")
        mocker.patch("datastore_api.lifespan.update_jobs", update_jobs_mock)

        poll_fts(icat_client)

        delete_many.assert_not_called()
        error.assert_called_once_with(
            "Unable to poll for job statuses: %s",
            "<urlopen error test>",
        )

    @pytest.mark.parametrize(
        ["statuses", "job_ids", "state", "file_state", "to_delete"],
        [
            pytest.param(
                [
                    {
                        "job_state": "SUBMITTED",
                        "files": [
                            {
                                "file_state": "SUBMITTED",
                                "source_surl": (
                                    "root://idc:8446//instrument/20XX/name-visitId/dataset/datafile?query"
                                ),
                            },
                        ],
                        "job_id": "0",
                    },
                    {"job_state": "CANCELLED", "files": [], "job_id": "1"},
                    {"job_state": "SUBMITTED", "files": [], "job_id": "2"},
                ],
                "0,2",
                "SUBMITTED",
                "SUBMITTED",
                0,
            ),
            pytest.param(
                [
                    {
                        "job_state": "FAILED",
                        "files": [
                            {
                                "file_state": "FAILED",
                                "source_surl": (
                                    "root://idc:8446//instrument/20XX/name-visitId/dataset/datafile?query"
                                ),
                            },
                        ],
                        "job_id": "0",
                    },
                    {"job_state": "FINISHED", "files": [], "job_id": "1"},
                    {"job_state": "FINISHEDDIRTY", "files": [], "job_id": "2"},
                ],
                "0,1,2",
                "FINISHEDDIRTY",
                "FAILED",
                1,
            ),
        ],
    )
    def test_update_job_ids(
        self,
        statuses: list[dict[str, str]],
        job_ids: str,
        state: str,
        file_state,
        to_delete: int,
        dataset_with_job_id: Entity,
        functional_icat_client: IcatClient,
        mocker: MockerFixture,
    ):
        get_fts3_client_mock = mocker.patch("datastore_api.lifespan.get_fts3_client")
        get_fts3_client_mock.return_value.status.return_value = statuses

        type_job_ids = functional_icat_client.settings.parameter_type_job_ids
        type_job_state = functional_icat_client.settings.parameter_type_job_state
        equals_job_ids = {"type.name": type_job_ids}
        equals_job_state = {"type.name": type_job_state}
        parameters = functional_icat_client.get_entities(
            entity="DatasetParameter",
            equals=equals_job_ids,
            includes="1",
        )

        beans_to_delete = update_jobs(
            icat_client=functional_icat_client,
            parameters=parameters,
        )

        calls = [
            call(job_id=["0", "1", "2"], list_files=True),
        ]

        get_fts3_client_mock.return_value.status.assert_has_calls(calls)

        parameter = functional_icat_client.get_single_entity(
            entity="DatasetParameter",
            equals=equals_job_ids,
            allow_empty=True,
        )
        assert parameter.stringValue == job_ids

        parameter = functional_icat_client.get_single_entity(
            entity="DatasetParameter",
            equals=equals_job_state,
            allow_empty=True,
        )
        assert parameter.stringValue == state

        parameter = functional_icat_client.get_single_entity(
            entity="DatafileParameter",
            equals=equals_job_state,
            allow_empty=True,
        )
        assert parameter.stringValue == file_state

        assert len(beans_to_delete) == to_delete


class TestStateCounter:
    @pytest.mark.parametrize(
        ["state", "expected_state"],
        [
            pytest.param("STAGING", "STAGING"),
            pytest.param("SUBMITTED", "SUBMITTED"),
            pytest.param("ACTIVE", "ACTIVE"),
            pytest.param("CANCELED", "CANCELED"),
            pytest.param("FAILED", "FAILED"),
            pytest.param("FINISHED", "FINISHED"),
            pytest.param("FINISHEDDIRTY", "FINISHEDDIRTY"),
        ],
    )
    def test_state_counter(self, state: str, expected_state: str):
        state_counter = StateCounter()
        state_counter.check_state(state=state, job_id="0")
        assert state_counter.state == expected_state
