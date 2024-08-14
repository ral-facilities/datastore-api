from datetime import datetime

import pytest
from pytest_mock import MockerFixture

from datastore_api.config import Fts3Settings, Settings
from datastore_api.models.icat import (
    FacilityCycleIdentifier,
    FacilityIdentifier,
    InstrumentIdentifier,
    Investigation,
    InvestigationTypeIdentifier,
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
        get_settings_mock = mocker.patch("datastore_api.models.icat.get_settings")
        fts3_settings = Fts3Settings(
            endpoint="https://localhost:8446",
            instrument_data_cache="root://idc.ac.uk:1094//",
            restored_data_cache="root://rdc.ac.uk:1094//",
            tape_archive="root://archive.ac.uk:1094//",
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
            facility=FacilityIdentifier(name="facility"),
            investigationType=InvestigationTypeIdentifier(name=investigation_type),
            instrument=InstrumentIdentifier(name="instrument"),
            facilityCycle=FacilityCycleIdentifier(name="20XX"),
            datasets=[],
        )

        assert investigation.releaseDate == expected_release_date
