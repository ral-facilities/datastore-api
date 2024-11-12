from pytest_mock import mocker, MockerFixture

from datastore_api.clients.icat_client import get_icat_cache, IcatCache, IcatClient
from datastore_api.controllers.investigation_archiver import InvestigationArchiver
from datastore_api.models.archive import ArchiveRequest
from datastore_api.models.icat import (
    FacilityCycleIdentifier,
    InstrumentIdentifier,
    Investigation,
    InvestigationTypeIdentifier,
)
from tests.fixtures import (
    archive_request,
    archive_request_parameters,
    archive_request_sample,
    icat_client,
    icat_client_empty_search,
    icat_settings,
    mock_fts3_settings,
    submit,
)


class TestInvestigationArchiver:
    def test_investigation_archiver(
        self,
        icat_client: IcatClient,
        icat_client_empty_search: IcatClient,
        archive_request: ArchiveRequest,
        mocker: MockerFixture,
    ):
        """Since we're asserting on the use of the IcatCache, we cannot assert that
        IcatCache is called once in multiple tests as it will only be called in the
        first test. Also cannot assert it is called once in first test and zero in
        subsequent tests as in principle filtering the selection of tests would then
        lead to failures if test_1 does not run before test_2. Therefore, run everything
        that asserts use of IcatCache in one function.
        """
        get_icat_cache.cache_clear()
        fts3_client = mocker.MagicMock(name="fts3_client")
        fts3_client.submit.return_value = "0"
        dataset = mocker.MagicMock(name="dataset")
        dataset.datafiles = [mocker.MagicMock(name="datafile")]

        investigation_archiver = InvestigationArchiver(
            icat_client,
            fts3_client,
            investigation=archive_request.investigation_identifier,
            datasets=[archive_request.dataset],
        )
        icat_client.client.new.return_value = dataset

        mock_investigation = mocker.MagicMock(name="investigation")
        mock_investigation.id = None
        icat_client_empty_search.client.new.return_value = mock_investigation
        investigation_archiver_empty_search = InvestigationArchiver(
            icat_client_empty_search,
            fts3_client,
            investigation=Investigation(
                title="title",
                investigationType=InvestigationTypeIdentifier(name="type"),
                facilityCycle=FacilityCycleIdentifier(name="facility"),
                instrument=InstrumentIdentifier(name="Instrument"),
                datasets=[archive_request.dataset],
                **archive_request.investigation_identifier.model_dump(),
            ),
        )
        icat_client_empty_search.client.new.return_value = dataset

        icat_cache_mock = mocker.MagicMock(wraps=IcatCache)
        mocker.patch("datastore_api.clients.icat_client.IcatCache", icat_cache_mock)
        get_icat_cache_mock = mocker.MagicMock(wraps=get_icat_cache)
        module = "datastore_api.clients.icat_client.get_icat_cache"
        mocker.patch(module, get_icat_cache_mock)

        investigation_archiver.archive_datasets()

        mock_call = mocker.call()
        assert investigation_archiver.job_ids == ["0"]
        assert len(investigation_archiver.beans) == 1
        assert "name='dataset'" in str(investigation_archiver.beans[0])
        icat_cache_mock.assert_called_once_with()
        get_icat_cache_mock.assert_has_calls([mock_call])

        investigation_archiver_empty_search.archive_datasets()

        assert investigation_archiver_empty_search.job_ids == ["0"]
        assert len(investigation_archiver_empty_search.beans) == 1
        assert "name='dataset'" in str(investigation_archiver.beans[0])
        icat_cache_mock.assert_called_once_with()
        get_icat_cache_mock.assert_has_calls([mock_call])
