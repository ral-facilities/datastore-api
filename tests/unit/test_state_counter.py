import pytest

from datastore_api.models.job import JobState
from datastore_api.state_counter import StateCounter
from tests.fixtures import SESSION_ID


class TestStateCounter:
    @pytest.mark.parametrize(
        ["state"],
        [
            pytest.param(JobState.active),
            pytest.param(JobState.archiving),
            pytest.param(JobState.canceled),
            pytest.param(JobState.failed),
            pytest.param(JobState.finished),
            pytest.param(JobState.finished_dirty),
            pytest.param(JobState.ready),
            pytest.param(JobState.staging),
            pytest.param(JobState.submitted),
            pytest.param("UNKNOWN"),
        ],
    )
    def test_state_counter(self, state: JobState):
        state_counter = StateCounter()
        state_counter.check_state(state=state, job_id=SESSION_ID)
        assert state_counter.state == state
