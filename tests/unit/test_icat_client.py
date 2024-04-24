from fastapi import HTTPException
import pytest

from datastore_api.icat_client import IcatClient
from datastore_api.models.login import Credentials, LoginRequest
from fixtures import icat_client, icat_client_empty_search, SESSION_ID


INSUFFICIENT_PERMISSIONS = (
    "fastapi.exceptions.HTTPException: 403: insufficient permissions"
)


class TestIcatClient:
    def test_build_path(self):
        assert IcatClient.build_path("a", "b", "c", "d") == "/a/b/c-d"

    def test_validate_entities(self):
        with pytest.raises(HTTPException) as e:
            IcatClient.validate_entities([], [1])

        assert e.exconly() == INSUFFICIENT_PERMISSIONS

    def test_login_success(self, icat_client: IcatClient):
        credentials = Credentials(username="root", password="pw")
        login_request = LoginRequest(auth="simple", credentials=credentials)
        session_id = icat_client.login(login_request=login_request)
        assert session_id == SESSION_ID
        assert icat_client.client.sessionId is None

    def test_login_failure(self, icat_client: IcatClient):
        credentials = Credentials(username="root", password="pw")
        login_request = LoginRequest(auth="simpl", credentials=credentials)
        with pytest.raises(HTTPException) as e:
            icat_client.login(login_request=login_request)

        assert e.exconly() == "fastapi.exceptions.HTTPException: 401: test"
        assert icat_client.client.sessionId is None

    def test_authorise_admin_failure(self, icat_client: IcatClient):
        with pytest.raises(HTTPException) as e:
            icat_client.authorise_admin(session_id=SESSION_ID)

        assert e.exconly() == INSUFFICIENT_PERMISSIONS
        assert icat_client.client.sessionId is None

    def test_get_investigation_paths(self, icat_client: IcatClient):
        paths = icat_client.get_investigation_paths(
            session_id=SESSION_ID,
            investigation_ids=[1],
        )

        # Don't assert the path as the Mocked object does not have meaningful attributes
        assert len(paths) == 1
        assert icat_client.client.sessionId is None

    def test_get_single_entity_failure(self, icat_client_empty_search: IcatClient):
        with pytest.raises(HTTPException) as e:
            icat_client_empty_search.get_single_entity(
                session_id=SESSION_ID,
                entity="Facility",
                conditions={"name": "facility"},
            )

        err = (
            "fastapi.exceptions.HTTPException: 400: "
            "No Facility with conditions {'name': 'facility'}"
        )
        assert e.exconly() == err

    def test_create_many(self, icat_client: IcatClient):
        icat_client.create_many(session_id=SESSION_ID, beans=[])
        icat_client.client.createMany.assert_called_once_with(beans=[])
