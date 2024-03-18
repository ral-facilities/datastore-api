from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials
import pytest

from datastore_api.auth import validate_session_id


SESSION_ID = "00000000-0000-0000-0000-000000000000"


class TestIcatClient:
    def test_validate_session_id_success(self):
        credentials = HTTPAuthorizationCredentials(
            scheme="Bearer",
            credentials=SESSION_ID,
        )
        assert validate_session_id(credentials) == SESSION_ID

    def test_validate_session_id_failure(self):
        credentials = HTTPAuthorizationCredentials(scheme="Bearer", credentials="")
        with pytest.raises(HTTPException) as e:
            validate_session_id(credentials)

        message = "fastapi.exceptions.HTTPException: 401: value not a valid UUID"
        assert e.exconly() == message
