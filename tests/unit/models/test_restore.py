from pydantic import ValidationError
import pytest

from datastore_api.models.restore import RestoreRequest


class TestRestore:
    def test_restore_request(self):
        with pytest.raises(ValidationError) as e:
            RestoreRequest(
                investigation_ids=[],
                dataset_ids=[],
                datafile_ids=[],
            )

        assert "At least one id must be provided" in e.exconly()
