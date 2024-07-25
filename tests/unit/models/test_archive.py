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
        "investigation_type, start_date, end_date, release_date, expected_release_date",
        [
            pytest.param(
                "type",
                datetime(2000, 1, 1),
                datetime(2010, 1, 1),
                datetime(2020, 1, 1),
                datetime(2020, 1, 1),
                id="All dates set",
            ),
            pytest.param(
                "type",
                datetime(2000, 1, 1),
                datetime(2010, 1, 1),
                None,
                datetime(2012, 1, 1),
                id="No release date",
            ),
            pytest.param(
                "type",
                datetime(2000, 1, 1),
                None,
                None,
                datetime(2002, 1, 1),
                id="No release or end date",
            ),
            pytest.param(
                "type",
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
            pytest.param(
                "commercial",
                datetime(2000, 1, 1),
                datetime(2010, 1, 1),
                datetime(2020, 1, 1),
                None,
                id="Commercial type",
            ),
        ],
    )
    def test_investigation(
        self,
        investigation_type: str,
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
            endpoint="https://127.0.0.1",
            instrument_data_cache="root://idc:1094//",
            user_data_cache="root://udc:1094//",
            tape_archive="root://archive:1094//",
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
            investigationType=InvestigationType(name=investigation_type),
            instrument=Instrument(name="instrument"),
            facilityCycle=FacilityCycle(name="20XX"),
            datasets=[],
        )

        assert investigation.releaseDate == expected_release_date
