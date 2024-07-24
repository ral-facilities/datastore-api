from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any

from annotated_types import Len
from pydantic import BaseModel, constr, Field, validator

from datastore_api.config import get_settings


ShortStr = constr(max_length=255)
LongStr = constr(max_length=4000)


class ParameterValueType(StrEnum):
    NUMERIC = "NUMERIC"
    STRING = "STRING"
    DATE_AND_TIME = "DATE_AND_TIME"


class FacilityIdentifier(BaseModel):
    name: ShortStr = Field(example="facility")


class InstrumentIdentifier(BaseModel):
    name: ShortStr = Field(example="instrument")


class FacilityCycleIdentifier(BaseModel):
    name: ShortStr = Field(example="20XX")


class DatasetTypeIdentifier(BaseModel):
    name: ShortStr = Field(example="type")


class InvestigationTypeIdentifier(BaseModel):
    name: ShortStr = Field(example="type")


class ParameterTypeIdentifier(BaseModel):
    name: ShortStr
    units: ShortStr


class ParameterType(ParameterTypeIdentifier):
    valueType: ParameterValueType
    pid: ShortStr = None
    description: ShortStr = None
    unitsFullName: ShortStr = None


class BaseParameter(BaseModel):
    parameter_type: ParameterTypeIdentifier

    def excluded_dict(self) -> dict[str, Any]:
        """Utility function for excluding fields which should not be passed to ICAT for
        entity creation as kwargs.

        Returns:
            dict[str, Any]: Dictionary of fields, excluding None values.
        """
        return self.dict(exclude={"parameter_type"}, exclude_none=True)


class StringParameter(BaseParameter):
    stringValue: LongStr


class NumericParameter(BaseParameter):
    numericValue: float
    error: float = None
    rangeBottom: float = None
    rangeTop: float = None


class DateTimeParameter(BaseParameter):
    dateTimeValue: datetime


Parameter = StringParameter | NumericParameter | DateTimeParameter


class SampleTypeIdentifier(BaseModel):
    name: ShortStr
    molecularFormula: ShortStr


class SampleType(SampleTypeIdentifier):
    safetyInformation: LongStr = None


class Sample(BaseModel):
    name: ShortStr
    pid: ShortStr = None

    # Relationships
    # Implicit relationship to Investigation for uniqueness constraint
    sample_type: SampleTypeIdentifier
    parameters: list[Parameter] = []

    def excluded_dict(self) -> dict[str, Any]:
        """Utility function for excluding fields which should not be passed to ICAT for
        entity creation as kwargs.

        Returns:
            dict[str, Any]: Dictionary of fields, excluding None values.
        """
        return self.dict(exclude={"sample_type", "parameters"}, exclude_none=True)


class TechniqueIdentifier(BaseModel):
    name: ShortStr


class Technique(TechniqueIdentifier):
    pid: ShortStr = None
    description: ShortStr = None


class DatafileFormatIdentifier(BaseModel):
    name: ShortStr
    version: ShortStr


class DatafileFormat(DatafileFormatIdentifier):
    datafile_format_type: ShortStr = None
    description: ShortStr = None


class Datafile(BaseModel):
    name: ShortStr
    description: ShortStr = Field(default=None, example="Description")
    doi: ShortStr = Field(default=None, example="10.00000/00000")
    fileSize: int = None
    checksum: ShortStr = None
    datafileCreateTime: datetime = None
    datafileModTime: datetime = None

    # Relationships
    datafileFormat: DatafileFormatIdentifier = None
    parameters: list[Parameter] = []

    def excluded_dict(self) -> dict[str, Any]:
        """Utility function for excluding fields which should not be passed to ICAT for
        entity creation as kwargs.

        Returns:
            dict[str, Any]: Dictionary of fields, excluding None values.
        """
        return self.dict(exclude={"datafileFormat", "parameters"}, exclude_none=True)


class Dataset(BaseModel):
    name: ShortStr
    complete: bool = True
    description: ShortStr = Field(default=None, example="Description")
    doi: ShortStr = Field(default=None, example="10.00000/00000")
    startDate: datetime = None
    endDate: datetime = None

    # Relationships
    datasetType: DatasetTypeIdentifier
    datafiles: Annotated[list[Datafile], Len(min_length=1)]
    sample: Sample = None
    parameters: list[Parameter] = []
    datasetTechniques: list[Technique] = []
    datasetInstruments: list[InstrumentIdentifier] = []

    def excluded_dict(self) -> dict[str, Any]:
        """Utility function for excluding fields which should not be passed to ICAT for
        entity creation as kwargs.

        Returns:
            dict[str, Any]:
                Dictionary of fields, excluding None values and related Entities.
        """
        exclude = {
            "datasetType",
            "datafiles",
            "sample",
            "parameters",
            "datasetTechniques",
            "datasetInstruments",
        }
        return self.dict(exclude=exclude, exclude_none=True)


class InvestigationIdentifier(BaseModel):
    """Only defines the attributes needed to satisfy the Investigation Uniqueness
    constraints.
    """

    name: ShortStr = Field(example="ABC123")
    visitId: ShortStr = Field(example="1")


class Investigation(InvestigationIdentifier):
    # Relationships
    investigationType: InvestigationTypeIdentifier
    instrument: InstrumentIdentifier
    facilityCycle: FacilityCycleIdentifier
    datasets: Annotated[list[Dataset], Len(min_length=1)]

    # Attributes
    title: ShortStr = Field(example="Title")
    summary: LongStr = Field(default=None, example="Summary")
    doi: ShortStr = Field(default=None, example="10.00000/00000")
    startDate: datetime = None
    endDate: datetime = None
    releaseDate: datetime = None

    @validator("releaseDate")
    def define_release_date(cls, v: datetime | None, values: dict) -> datetime:
        if values["investigationType"].name in get_settings().icat.embargo_types:
            return None

        if v is not None:
            return v

        if "endDate" in values and values["endDate"] is not None:
            date = values["endDate"]
        elif "startDate" in values and values["startDate"] is not None:
            date = values["startDate"]
        else:
            date = datetime.today()
        return datetime(
            year=date.year + get_settings().icat.embargo_period_years,
            month=date.month,
            day=date.day,
            tzinfo=date.tzinfo,
        )

    def excluded_dict(self) -> dict[str, Any]:
        """Utility function for excluding fields which should not be passed to ICAT for
        entity creation as kwargs.

        Returns:
            dict[str, Any]:
                Dictionary of fields, excluding None values and related Entities.
        """
        exclude = {
            "facility",
            "investigationType",
            "instrument",
            "facilityCycle",
            "datasets",
        }
        return self.dict(exclude=exclude, exclude_none=True)
