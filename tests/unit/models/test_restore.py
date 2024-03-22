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

        expected_message = (
            "pydantic.error_wrappers.ValidationError: "
            "1 validation error for RestoreRequest\n"
            "__root__\n"
            "  At least one id must be provided (type=value_error)"
        )
        assert e.exconly() == expected_message
