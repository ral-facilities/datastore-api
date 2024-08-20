from datetime import datetime
from typing import Generator
from unittest.mock import MagicMock

import fts3.rest.client.easy as fts3
from icat import ICATObjectExistsError, ICATSessionError
from icat.entity import Entity
from pydantic import ValidationError
import pytest
from pytest_mock import mocker, MockerFixture

from datastore_api.config import (
    Fts3Settings,
    FunctionalUser,
    get_settings,
    IcatSettings,
    S3Settings,
    Settings,
)
from datastore_api.icat_client import IcatClient
from datastore_api.models.archive import ArchiveRequest
from datastore_api.models.icat import (
    Datafile,
    DatafileFormatIdentifier,
    Dataset,
    DatasetTypeIdentifier,
    DateTimeParameter,
    FacilityCycleIdentifier,
    FacilityIdentifier,
    InstrumentIdentifier,
    InvestigationIdentifier,
    NumericParameter,
    Parameter,
    ParameterTypeIdentifier,
    Sample,
    SampleTypeIdentifier,
    StringParameter,
    TechniqueIdentifier,
)
from datastore_api.s3_client import S3Client


SESSION_ID = "00000000-0000-0000-0000-000000000000"


@pytest.fixture(scope="function")
def submit(mocker: MockerFixture) -> MagicMock:
    try:
        get_settings()
        submit_mock = mocker.MagicMock(wraps=fts3.submit)
    except ValidationError:
        submit_mock = mocker.MagicMock()
        submit_mock.return_value = SESSION_ID

    mocker.patch("datastore_api.fts3_client.fts3.submit", submit_mock)
    return submit_mock


@pytest.fixture(scope="function")
def mock_fts3_settings(submit: MagicMock, mocker: MockerFixture) -> Settings:
    try:
        settings = get_settings()
    except ValidationError:
        # Assume the issue is that we do not have the cert to communicate with FTS.
        # This will be the case for GHA workflows, in which case,
        # pass a readable file to satisfy the validator and mock requests to FTS.
        fts3_settings = Fts3Settings(
            endpoint="https://fts-test01.gridpp.rl.ac.uk:8446",
            instrument_data_cache="root://idc.ac.uk:1094//",
            restored_data_cache="root://rdc.ac.uk:1094//",
            tape_archive="root://archive.ac.uk:1094//",
            x509_user_cert=__file__,
            x509_user_key=__file__,
        )
        settings = Settings(fts3=fts3_settings)

        mocker.patch("datastore_api.fts3_client.fts3.Context")

        fts_status_mock = mocker.patch("datastore_api.fts3_client.fts3.get_job_status")
        fts_status_mock.return_value = {"status": {}}

    for module in {"fts3_client", "icat_client", "s3_client", "models.icat"}:
        get_settings_mock = mocker.patch(f"datastore_api.{module}.get_settings")
        get_settings_mock.return_value = settings

    fts_status_mock = mocker.patch("datastore_api.fts3_client.fts3.get_jobs_statuses")
    # This represents a single status dict, which is returned if a single job_id is used
    fts_status_mock.return_value = {"status": {}}

    fts_cancel_mock = mocker.patch("datastore_api.fts3_client.fts3.cancel")
    fts_cancel_mock.return_value = "CANCELED"

    return settings


@pytest.fixture(scope="function")
def archive_request_parameters() -> list[Parameter]:
    string_type = ParameterTypeIdentifier(name="string", units="")
    numeric_type = ParameterTypeIdentifier(name="numeric", units="")
    date_time_type = ParameterTypeIdentifier(name="date_time", units="")
    return [
        StringParameter(stringValue="stringValue", parameter_type=string_type),
        NumericParameter(
            numericValue=0,
            error=0,
            rangeBottom=-1,
            rangeTop=1,
            parameter_type=numeric_type,
        ),
        DateTimeParameter(dateTimeValue=datetime.now(), parameter_type=date_time_type),
    ]


@pytest.fixture(scope="function")
def archive_request_sample(archive_request_parameters: list[Parameter]) -> Sample:
    sample_type = SampleTypeIdentifier(name="carbon", molecularFormula="C")
    return Sample(
        name="sample",
        sample_type=sample_type,
        parameters=archive_request_parameters,
    )


@pytest.fixture(scope="function")
def archive_request(
    archive_request_parameters: list[Parameter],
    archive_request_sample: Sample,
    mocker: MockerFixture,
) -> ArchiveRequest:
    try:
        get_settings()
    except ValidationError:
        # Assume the issue is that we do not have the cert to communicate with FTS.
        # This will be the case for GHA workflows, in which case,
        # pass a readable file to satisfy the validator and mock requests to FTS.
        fts3_settings = Fts3Settings(
            endpoint="https://fts-test01.gridpp.rl.ac.uk:8446",
            instrument_data_cache="root://idc.ac.uk:1094//",
            restored_data_cache="root://rdc.ac.uk:1094//",
            tape_archive="root://archive.ac.uk:1094//",
            x509_user_cert=__file__,
            x509_user_key=__file__,
        )
        settings = Settings(fts3=fts3_settings)

        get_settings_mock = mocker.patch("datastore_api.models.icat.get_settings")
        get_settings_mock.return_value = settings

    investigation_identifier = InvestigationIdentifier(name="name", visitId="visitId")
    datafile = Datafile(
        name="datafile",
        datafileFormat=DatafileFormatIdentifier(name="txt", version="0"),
        parameters=archive_request_parameters,
    )
    dataset = Dataset(
        name="dataset1",
        datasetType=DatasetTypeIdentifier(name="type"),
        datafiles=[datafile],
        sample=archive_request_sample,
        parameters=archive_request_parameters,
        datasetTechniques=[TechniqueIdentifier(name="technique")],
        datasetInstruments=[InstrumentIdentifier(name="instrument")],
    )

    return ArchiveRequest(
        facility_identifier=FacilityIdentifier(name="facility"),
        instrument_identifier=InstrumentIdentifier(name="instrument"),
        facility_cycle_identifier=FacilityCycleIdentifier(name="20XX"),
        investigation_identifier=investigation_identifier,
        dataset=dataset,
    )


def login_side_effect(auth: str, credentials: dict) -> str:
    if auth == "simple":
        return SESSION_ID

    raise ICATSessionError("test")


@pytest.fixture(scope="function")
def icat_settings(mock_fts3_settings: Settings):
    functional_user = FunctionalUser(auth="simple", username="root", password="pw")
    mock_fts3_settings.icat.functional_user = functional_user
    return mock_fts3_settings.icat


@pytest.fixture(scope="function")
def icat_client(icat_settings: IcatSettings, mocker: MockerFixture):
    client = mocker.patch("datastore_api.icat_client.Client")
    client.return_value.login.side_effect = login_side_effect
    client.return_value.getUserName.return_value = "simple/root"
    client.return_value.search.return_value = [mocker.MagicMock()]

    mocker.patch("datastore_api.icat_client.Query")

    return IcatClient()


@pytest.fixture(scope="function")
def icat_client_empty_search(icat_settings: IcatSettings, mocker: MockerFixture):
    def generator():
        yield []
        while True:
            yield [mocker.MagicMock()]

    iterator = generator()

    def search_side_effect(**kwargs):
        return next(iterator)

    client = mocker.patch("datastore_api.icat_client.Client")
    client.return_value.login.side_effect = login_side_effect
    client.return_value.getUserName.return_value = "simple/root"
    client.return_value.search.side_effect = search_side_effect

    mocker.patch("datastore_api.icat_client.Query")

    return IcatClient()


@pytest.fixture(scope="function")
def functional_icat_client(
    mock_fts3_settings: Settings,
) -> Generator[IcatClient, None, None]:
    icat_client = IcatClient()
    icat_client.login_functional()

    yield icat_client

    beans = [
        *icat_client.get_entities(entity="Investigation", equals={"name": "name"}),
        *icat_client.get_entities(entity="Dataset", equals={"name": "dataset"}),
        *icat_client.get_entities(entity="Datafile", equals={"name": "datafile"}),
    ]
    icat_client.client.deleteMany(beans)


@pytest.fixture(scope="function")
def s3_settings(mock_fts3_settings: Settings):
    return mock_fts3_settings.s3


@pytest.fixture(scope="function")
def s3_client(s3_settings: S3Settings, mocker: MockerFixture):
    return S3Client()


@pytest.fixture(scope="function")
def facility(functional_icat_client: IcatClient) -> Generator[Entity, None, None]:
    facility = create(
        icat_client=functional_icat_client,
        entity="Facility",
        name="facility",
    )

    yield facility

    delete(icat_client=functional_icat_client, entity=facility)


@pytest.fixture(scope="function")
def datafile_format(
    functional_icat_client: IcatClient,
    facility: Entity,
) -> Generator[Entity, None, None]:
    datafile_format = create(
        icat_client=functional_icat_client,
        entity="DatafileFormat",
        name="txt",
        version="0",
        facility=facility,
    )

    yield datafile_format

    delete(icat_client=functional_icat_client, entity=datafile_format)


@pytest.fixture(scope="function")
def dataset_type(
    functional_icat_client: IcatClient,
    facility: Entity,
) -> Generator[Entity, None, None]:
    dataset_type = create(
        icat_client=functional_icat_client,
        entity="DatasetType",
        name="type",
        facility=facility,
    )

    yield dataset_type

    delete(icat_client=functional_icat_client, entity=dataset_type)


@pytest.fixture(scope="function")
def investigation_type(
    functional_icat_client: IcatClient,
    facility: Entity,
) -> Generator[Entity, None, None]:
    investigation_type = create(
        icat_client=functional_icat_client,
        entity="InvestigationType",
        name="type",
        facility=facility,
    )

    yield investigation_type

    delete(icat_client=functional_icat_client, entity=investigation_type)


@pytest.fixture(scope="function")
def facility_cycle(
    functional_icat_client: IcatClient,
    facility: Entity,
) -> Generator[Entity, None, None]:
    facility_cycle = create(
        icat_client=functional_icat_client,
        entity="FacilityCycle",
        name="20XX",
        facility=facility,
    )

    yield facility_cycle

    delete(icat_client=functional_icat_client, entity=facility_cycle)


@pytest.fixture(scope="function")
def instrument(
    functional_icat_client: IcatClient,
    facility: Entity,
) -> Generator[Entity, None, None]:
    instrument = create(
        icat_client=functional_icat_client,
        entity="Instrument",
        name="instrument",
        facility=facility,
    )

    yield instrument

    delete(icat_client=functional_icat_client, entity=instrument)


@pytest.fixture(scope="function")
def parameter_type_state(
    functional_icat_client: IcatClient,
    facility: Entity,
) -> Generator[Entity, None, None]:
    parameter_type = create(
        icat_client=functional_icat_client,
        entity="ParameterType",
        name="Archival state",
        facility=facility,
        units="",
        valueType="STRING",
        applicableToDataset=True,
        applicableToDatafile=True,
    )

    yield parameter_type

    delete(icat_client=functional_icat_client, entity=parameter_type)


@pytest.fixture(scope="function")
def parameter_type_job_ids(
    functional_icat_client: IcatClient,
    facility: Entity,
) -> Generator[Entity, None, None]:
    parameter_type = create(
        icat_client=functional_icat_client,
        entity="ParameterType",
        name="Archival ids",
        facility=facility,
        units="",
        valueType="STRING",
        applicableToDataset=True,
    )

    yield parameter_type

    delete(icat_client=functional_icat_client, entity=parameter_type)


@pytest.fixture(scope="function")
def parameter_type_string(
    functional_icat_client: IcatClient,
    facility: Entity,
) -> Generator[Entity, None, None]:
    parameter_type = create(
        icat_client=functional_icat_client,
        entity="ParameterType",
        name="string",
        facility=facility,
        units="",
        valueType="STRING",
        applicableToDataset=True,
        applicableToDatafile=True,
        applicableToSample=True,
    )

    yield parameter_type

    delete(icat_client=functional_icat_client, entity=parameter_type)


@pytest.fixture(scope="function")
def parameter_type_numeric(
    functional_icat_client: IcatClient,
    facility: Entity,
) -> Generator[Entity, None, None]:
    parameter_type = create(
        icat_client=functional_icat_client,
        entity="ParameterType",
        name="numeric",
        facility=facility,
        units="",
        valueType="NUMERIC",
        applicableToDataset=True,
        applicableToDatafile=True,
        applicableToSample=True,
    )

    yield parameter_type

    delete(icat_client=functional_icat_client, entity=parameter_type)


@pytest.fixture(scope="function")
def parameter_type_date_time(
    functional_icat_client: IcatClient,
    facility: Entity,
) -> Generator[Entity, None, None]:
    parameter_type = create(
        icat_client=functional_icat_client,
        entity="ParameterType",
        name="date_time",
        facility=facility,
        units="",
        valueType="DATE_AND_TIME",
        applicableToDataset=True,
        applicableToDatafile=True,
        applicableToSample=True,
    )

    yield parameter_type

    delete(icat_client=functional_icat_client, entity=parameter_type)


@pytest.fixture(scope="function")
def sample_type(
    functional_icat_client: IcatClient,
    facility: Entity,
) -> Generator[Entity, None, None]:
    sample_type = create(
        icat_client=functional_icat_client,
        entity="SampleType",
        name="carbon",
        facility=facility,
        molecularFormula="C",
    )

    yield sample_type

    delete(icat_client=functional_icat_client, entity=sample_type)


@pytest.fixture(scope="function")
def technique(
    functional_icat_client: IcatClient,
) -> Generator[Entity, None, None]:
    technique = create(
        icat_client=functional_icat_client,
        entity="Technique",
        name="technique",
    )

    yield technique

    delete(icat_client=functional_icat_client, entity=technique)


@pytest.fixture(scope="function")
def investigation(
    functional_icat_client: IcatClient,
    facility: Entity,
    investigation_type: Entity,
    instrument: Entity,
    facility_cycle: Entity,
    dataset_type: Entity,
) -> Generator[Entity, None, None]:
    investigation_instrument = functional_icat_client.client.new(
        obj="InvestigationInstrument",
        instrument=instrument,
    )
    investigation_facility_cycle = functional_icat_client.client.new(
        obj="InvestigationFacilityCycle",
        facilityCycle=facility_cycle,
    )
    datafile = functional_icat_client.client.new(
        obj="Datafile",
        name="datafile",
        location="instrument/20XX/name-visitId/type/dataset/datafile",
    )
    dataset = functional_icat_client.client.new(
        obj="Dataset",
        name="dataset",
        type=dataset_type,
        datafiles=[datafile],
    )
    investigation = create(
        icat_client=functional_icat_client,
        entity="Investigation",
        name="name",
        visitId="visitId",
        title="title",
        summary="summary",
        startDate=datetime.now(),
        endDate=datetime.now(),
        releaseDate=datetime.now(),
        facility=facility,
        type=investigation_type,
        investigationInstruments=[investigation_instrument],
        investigationFacilityCycles=[investigation_facility_cycle],
        datasets=[dataset],
    )

    yield investigation

    delete(icat_client=functional_icat_client, entity=investigation)


@pytest.fixture(scope="function")
def investigation_tear_down(
    functional_icat_client: IcatClient,
) -> Generator[None, None, None]:
    yield None

    investigation = functional_icat_client.get_single_entity(
        entity="Investigation",
        equals={"name": "name", "visitId": "visitId"},
        allow_empty=True,
    )
    if investigation is not None:
        delete(icat_client=functional_icat_client, entity=investigation)


@pytest.fixture(scope="function")
def bucket_creation() -> Generator[str, None, None]:
    bucket = S3Client().create_bucket()
    yield bucket["Location"][1:]


@pytest.fixture(scope="function")
def bucket_deletion() -> Generator[None, None, None]:
    yield None

    for bucket in S3Client().list_buckets():
        if bucket != "miniotestbucket":
            S3Client().delete_bucket(bucket)


@pytest.fixture(scope="function")
def tag_bucket() -> Generator[None, None, None]:
    tags = [{"Key": "00000000-0000-0000-0000-000000000000", "Value": "STAGING"}]
    S3Client().tag_bucket(bucket_name="miniotestbucket", tags=tags)


@pytest.fixture(scope="function")
def dataset_with_job_id(
    functional_icat_client: IcatClient,
    dataset_type: Entity,
    parameter_type_job_ids: Entity,
    parameter_type_state: Entity,
    investigation: Entity,
) -> Generator[Entity, None, None]:
    parameter_job_ids = functional_icat_client.client.new(
        obj="DatasetParameter",
        stringValue="0,1,2",
        type=parameter_type_job_ids,
    )
    parameter_state = functional_icat_client.client.new(
        obj="DatasetParameter",
        stringValue="SUBMITTED",
        type=parameter_type_state,
    )
    parameter_file_state = functional_icat_client.client.new(
        obj="DatafileParameter",
        stringValue="SUBMITTED",
        type=parameter_type_state,
    )
    datafile = functional_icat_client.client.new(
        obj="Datafile",
        name="datafile",
        location="instrument/20XX/name-visitId/dataset/datafile",
        parameters=[parameter_file_state],
    )
    dataset = create(
        icat_client=functional_icat_client,
        entity="Dataset",
        name="dataset1",
        type=dataset_type,
        investigation=investigation,
        parameters=[parameter_job_ids, parameter_state],
        datafiles=[datafile],
    )

    yield dataset

    delete(icat_client=functional_icat_client, entity=dataset)


def create(icat_client: IcatClient, entity: str, **kwargs) -> Entity:
    try:
        icat_entity = icat_client.client.new(obj=entity, **kwargs)
        icat_entity_id = icat_client.client.create(icat_entity)
        icat_entity.id = icat_entity_id

    except ICATObjectExistsError:
        equals = {"name": kwargs["name"]}
        if "facility" in kwargs:
            equals["facility.name"] = kwargs["facility"].name

        icat_entity = icat_client.get_single_entity(entity=entity, equals=equals)

    return icat_entity


def delete(icat_client: IcatClient, entity: Entity) -> None:
    icat_client.client.delete(entity)
