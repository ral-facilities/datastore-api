from pytest_mock import mocker, MockerFixture

from datastore_api.icat_client import IcatClient
from datastore_api.investigation_archiver import InvestigationArchiver
from datastore_api.models.archive import Investigation
from tests.fixtures import (
    icat_client,
    icat_client_empty_search,
    icat_settings,
    investigation_metadata,
)


class TestInvestigationArchiver:
    def test_investigation_archiver(
        self,
        icat_client: IcatClient,
        investigation_metadata: Investigation,
        mocker: MockerFixture,
    ):
        fts3_client = mocker.MagicMock(name="fts3_client")
        fts3_client.submit.return_value = "0"
        investigation_archiver = InvestigationArchiver(
            icat_client,
            fts3_client,
            investigation_metadata,
        )
        icat_client.client.new.return_value = mocker.MagicMock(name="dataset")
        investigation_archiver.archive_datasets()
        assert investigation_archiver.job_ids == ["0"]
        assert len(investigation_archiver.beans) == 1
        assert "name='dataset'" in str(investigation_archiver.beans[0])

    def test_investigation_archiver_no_investigation(
        self,
        icat_client_empty_search: IcatClient,
        investigation_metadata: Investigation,
        mocker: MockerFixture,
    ):
        mock_investigation = mocker.MagicMock(name="investigation")
        mock_investigation.id = None
        icat_client_empty_search.client.new.return_value = mock_investigation
        fts3_client = mocker.MagicMock(name="fts3_client")
        fts3_client.submit.return_value = "0"
        investigation_archiver = InvestigationArchiver(
            icat_client_empty_search,
            fts3_client,
            investigation_metadata,
        )
        dataset = mocker.MagicMock(name="dataset")
        icat_client_empty_search.client.new.return_value = dataset
        investigation_archiver.archive_datasets()
        assert investigation_archiver.job_ids == ["0"]
        assert len(investigation_archiver.beans) == 1
        assert "name='investigation'" in str(investigation_archiver.beans[0])
