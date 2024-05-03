from unittest.mock import call

from icat.entity import Entity
import pytest
from pytest_mock import MockerFixture

from datastore_api.fts3_client import get_fts3_client
from datastore_api.icat_client import IcatClient
from datastore_api.lifespan import lifespan, StateCounter, update_jobs
from tests.fixtures import (
    dataset_type,
    dataset_with_job_id,
    facility,
    facility_cycle,
    functional_icat_client,
    instrument,
    investigation,
    investigation_type,
    parameter_type_job_ids,
    parameter_type_state,
)


class TestLifespan:
    async def test_lifespan(self):
        context_manager = lifespan(None)
        generator = context_manager.func(*context_manager.args)

        assert await generator.__anext__() is None

        with pytest.raises(StopAsyncIteration):
            await generator.__anext__()

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
                    },
                    {"job_state": "CANCELLED", "files": []},
                    {"job_state": "SUBMITTED", "files": []},
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
                    },
                    {"job_state": "FINISHED", "files": []},
                    {"job_state": "FINISHEDDIRTY", "files": []},
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
        mocker: MockerFixture,
    ):
        get_fts3_client_mock = mocker.patch("datastore_api.lifespan.get_fts3_client")
        get_fts3_client_mock.return_value.status.side_effect = statuses

        icat_client = IcatClient()
        icat_client.login_functional()
        parameters = icat_client.get_entities(
            entity="DatasetParameter",
            equals={"type.name": icat_client.settings.parameter_type_job_ids},
            includes="1",
        )
        print(parameters)

        beans_to_delete = update_jobs(
            icat_client=icat_client,
            parameters=parameters,
        )

        calls = [
            call(job_id="0", list_files=True),
            call(job_id="1", list_files=True),
            call(job_id="2", list_files=True),
        ]
        get_fts3_client_mock.return_value.status.assert_has_calls(calls)

        parameter = icat_client.get_single_entity(
            entity="DatasetParameter",
            equals={"type.name": icat_client.settings.parameter_type_job_ids},
            allow_empty=True,
        )
        assert parameter.stringValue == job_ids

        parameter = icat_client.get_single_entity(
            entity="DatasetParameter",
            equals={"type.name": icat_client.settings.parameter_type_job_state},
            allow_empty=True,
        )
        assert parameter.stringValue == state

        parameter = icat_client.get_single_entity(
            entity="DatafileParameter",
            equals={"type.name": icat_client.settings.parameter_type_job_state},
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
