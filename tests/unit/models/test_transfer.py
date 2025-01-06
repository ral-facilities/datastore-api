from pydantic import ValidationError
import pytest

from datastore_api.models.transfer import TransferRequest


class TestTransfer:
    def test_transfer_request(self):
        with pytest.raises(ValidationError) as e:
            TransferRequest(
                investigation_ids=[],
                dataset_ids=[],
                datafile_ids=[],
            )

        assert "At least one id must be provided" in e.exconly()
