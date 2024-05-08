from pytest_mock import mocker, MockerFixture

from datastore_api.icat_client import get_icat_cache, IcatCache, IcatClient
from datastore_api.investigation_archiver import InvestigationArchiver
from datastore_api.models.archive import Investigation
from tests.fixtures import (
    icat_client,
    icat_client_empty_search,
    icat_settings,
    investigation_metadata,
    mock_fts3_settings,
)


class TestInvestigationArchiver:
    def test_investigation_archiver(
        self,
        icat_client: IcatClient,
        icat_client_empty_search: IcatClient,
        investigation_metadata: Investigation,
        mocker: MockerFixture,
    ):
        """Since we're asserting on the use of the IcatCache, we cannot assert that
        IcatCache is called once in multiple tests as it will only be called in the
        first test. Also cannot assert it is called once in first test and zero in
        subsequent tests as in principle filtering the selection of tests would then
        lead to failures if test_1 does not run before test_2. Therefore, run everything
        that asserts use of IcatCache in one function.
        """
        fts3_client = mocker.MagicMock(name="fts3_client")
        fts3_client.submit.return_value = "0"

        investigation_archiver = InvestigationArchiver(
            icat_client,
            fts3_client,
            investigation_metadata,
        )
        icat_client.client.new.return_value = mocker.MagicMock(name="dataset")

        mock_investigation = mocker.MagicMock(name="investigation")
        mock_investigation.id = None
        icat_client_empty_search.client.new.return_value = mock_investigation
        investigation_archiver_empty_search = InvestigationArchiver(
            icat_client_empty_search,
            fts3_client,
            investigation_metadata,
        )
        dataset = mocker.MagicMock(name="dataset")
        icat_client_empty_search.client.new.return_value = dataset

        icat_cache_mock = mocker.MagicMock(wraps=IcatCache)
        mocker.patch("datastore_api.icat_client.IcatCache", icat_cache_mock)
        get_icat_cache_mock = mocker.MagicMock(wraps=get_icat_cache)
        mocker.patch("datastore_api.icat_client.get_icat_cache", get_icat_cache_mock)

        investigation_archiver.archive_datasets()

        assert investigation_archiver.job_ids == ["0"]
        assert len(investigation_archiver.beans) == 1
        assert "name='dataset'" in str(investigation_archiver.beans[0])
        icat_cache_mock.assert_called_once_with(facility_name="facility")
        mock_call = mocker.call(facility_name="facility")
        get_icat_cache_mock.assert_has_calls([mock_call] * 2)

        investigation_archiver_empty_search.archive_datasets()

        assert investigation_archiver_empty_search.job_ids == ["0"]
        assert len(investigation_archiver_empty_search.beans) == 1
        investigation_bean = investigation_archiver_empty_search.beans[0]
        assert "name='investigation'" in str(investigation_bean)
        icat_cache_mock.assert_called_once_with(facility_name="facility")
        get_icat_cache_mock.assert_has_calls([mock_call] * 4)
