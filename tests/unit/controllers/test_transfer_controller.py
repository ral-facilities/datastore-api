from fastapi import HTTPException
import pytest

from datastore_api.controllers.transfer_controller import DatasetReArchiver
from datastore_api.models.dataset import DatasetStatusListFilesResponse
from datastore_api.models.job import JobState


class TestDatasetReArchiver:
    @pytest.mark.parametrize(
        ["state", "error"],
        [
            pytest.param(
                JobState.active,
                "fastapi.exceptions.HTTPException: 400: "
                "Archival not yet complete, cannot retry",
            ),
            pytest.param(
                JobState.finished,
                "fastapi.exceptions.HTTPException: 400: "
                "Archival completed successfully, nothing to retry",
            ),
        ],
    )
    def test_validate_status(self, state: JobState, error: str):
        status = DatasetStatusListFilesResponse(state=state, file_states={})
        with pytest.raises(HTTPException) as e:
            DatasetReArchiver._validate_status(status)

        assert e.exconly() == error
