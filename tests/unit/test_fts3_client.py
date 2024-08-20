import pytest

from datastore_api.config import Settings, VerifyChecksum
from datastore_api.fts3_client import Fts3Client
from tests.fixtures import mock_fts3_settings, SESSION_ID, submit


class TestFts3Client:
    @pytest.mark.parametrize(
        ["verify_checksum", "checksum", "expected"],
        [
            pytest.param(VerifyChecksum.NONE, "ADLER32:1234", None),
            pytest.param(VerifyChecksum.NONE, "ADLER32", None),
            pytest.param(VerifyChecksum.NONE, None, None),
            pytest.param(VerifyChecksum.SOURCE, "ADLER32:1234", "ADLER32:1234"),
            pytest.param(VerifyChecksum.SOURCE, "ADLER32", None),
            pytest.param(VerifyChecksum.SOURCE, None, None),
            pytest.param(VerifyChecksum.DESTINATION, "ADLER32:1234", "ADLER32:1234"),
            pytest.param(VerifyChecksum.DESTINATION, "ADLER32", None),
            pytest.param(VerifyChecksum.DESTINATION, None, None),
            pytest.param(VerifyChecksum.BOTH, "ADLER32:1234", "ADLER32:1234"),
            pytest.param(VerifyChecksum.BOTH, "ADLER32", "ADLER32"),
            pytest.param(VerifyChecksum.BOTH, None, None),
            pytest.param(VerifyChecksum.BOTH, "UNKNOWN:1234", None),
        ],
    )
    def test_validate_checksum(
        self,
        verify_checksum: VerifyChecksum,
        checksum: str,
        expected: str,
        mock_fts3_settings: Settings,
    ):
        fts3_client = Fts3Client()
        fts3_client.supported_checksums = ["ADLER32"]
        fts3_client.verify_checksum = verify_checksum
        checksum = fts3_client._validate_checksum(checksum)
        assert checksum == expected

    def test_statuses(self, mock_fts3_settings: Settings):
        fts3_client = Fts3Client()
        statuses = fts3_client.statuses([SESSION_ID])
        assert isinstance(statuses, list)
