import json

from fastapi.testclient import TestClient
import pytest

from datastore_api.main import app


@pytest.fixture()
def test_client():
    return TestClient(app)


class TestMain:
    def test_version(self, test_client: TestClient):
        test_response = test_client.get("/version")

        assert test_response.status_code == 200
        assert json.loads(test_response.content) == {"version": "0.1.0"}

    def test_login_success(self, test_client: TestClient):
        credentials = {"username": "root", "password": "pw"}
        login_request = {"auth": "simple", "credentials": credentials}
        test_response = test_client.post("/login", content=json.dumps(login_request))

        assert test_response.status_code == 200
        assert list(json.loads(test_response.content).keys()) == ["sessionId"]

    @pytest.mark.parametrize(
        "login_request, detail",
        [
            pytest.param(
                {
                    "auth": "simple",
                    "credentials": {"username": "root", "password": "p"},
                },
                "The username and password do not match ",
                id="Bad credentials",
            ),
            pytest.param(
                {
                    "auth": "simpl",
                    "credentials": {"username": "root", "password": "pw"},
                },
                "Authenticator mnemonic simpl not recognised",
                id="Bad auth",
            ),
        ],
    )
    def test_login_failure(
        self,
        test_client: TestClient,
        login_request: dict,
        detail: str,
    ):
        test_response = test_client.post("/login", content=json.dumps(login_request))

        assert test_response.status_code == 401
        assert json.loads(test_response.content)["detail"] == detail
