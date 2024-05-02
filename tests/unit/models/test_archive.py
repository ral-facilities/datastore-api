from datetime import datetime

import pytest
from pytest_mock import MockerFixture

from datastore_api.config import Fts3Settings, Settings
from datastore_api.models.archive import (
    Facility,
    FacilityCycle,
    Instrument,
    Investigation,
    InvestigationType,
)


class TestArchive:
    @pytest.mark.parametrize(
        "start_date, end_date, release_date, expected_release_date",
        [
            pytest.param(
                datetime(2000, 1, 1),
                datetime(2010, 1, 1),
                datetime(2020, 1, 1),
                datetime(2020, 1, 1),
                id="All dates set",
            ),
            pytest.param(
                datetime(2000, 1, 1),
                datetime(2010, 1, 1),
                None,
                datetime(2012, 1, 1),
                id="No release date",
            ),
            pytest.param(
                datetime(2000, 1, 1),
                None,
                None,
                datetime(2002, 1, 1),
                id="No release or end date",
            ),
            pytest.param(
                None,
                None,
                None,
                datetime(
                    datetime.today().year + 2,
                    datetime.today().month,
                    datetime.today().day,
                ),
                id="No dates set",
            ),
        ],
    )
    def test_investigation(
        self,
        start_date: datetime,
        end_date: datetime,
        release_date: datetime,
        expected_release_date: datetime,
        mocker: MockerFixture,
    ):
        # For GHA workflows will not have certificate files,
        # pass a readable file to satisfy the validator.
        get_settings_mock = mocker.patch("datastore_api.models.archive.get_settings")
        fts3_settings = Fts3Settings(
            endpoint="",
            instrument_data_cache="",
            user_data_cache="",
            tape_archive="",
            x509_user_cert=__file__,
            x509_user_key=__file__,
        )
        get_settings_mock.return_value = Settings(fts3=fts3_settings)

        investigation = Investigation(
            name="name",
            visitId="visitId",
            title="title",
            summary="summary",
            doi="doi",
            startDate=start_date,
            endDate=end_date,
            releaseDate=release_date,
            facility=Facility(name="facility"),
            investigationType=InvestigationType(name="type"),
            instrument=Instrument(name="instrument"),
            facilityCycle=FacilityCycle(name="20XX"),
            datasets=[],
        )

        assert investigation.releaseDate == expected_release_date
