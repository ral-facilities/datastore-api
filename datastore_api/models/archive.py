from datetime import datetime
from typing import Annotated, Any

from annotated_types import Len
from pydantic import BaseModel, Field, validator

from datastore_api.config import get_settings


class Facility(BaseModel):
    name: str = Field(example="facility")


class Instrument(BaseModel):
    name: str = Field(example="instrument")


class FacilityCycle(BaseModel):
    name: str = Field(example="20XX")


class DatasetType(BaseModel):
    name: str = Field(example="type")


class InvestigationType(BaseModel):
    name: str = Field(example="type")


class Datafile(BaseModel):
    name: str
    description: str = Field(default=None, example="Description")
    doi: str = Field(default=None, example="10.00000/00000")
    location: str = None
    fileSize: int = None
    checksum: str = None
    datafileCreateTime: datetime = None
    datafileModTime: datetime = None

    def excluded_dict(self) -> dict[str, Any]:
        """Utility function for excluding fields which should not be passed to ICAT for
        entity creation as kwargs.

        Returns:
            dict[str, Any]: Dictionary of fields, excluding None values.
        """
        return self.dict(exclude_none=True)


class Dataset(BaseModel):
    name: str
    complete: bool = True
    description: str = Field(default=None, example="Description")
    doi: str = Field(default=None, example="10.00000/00000")
    location: str = None
    startDate: datetime = None
    endDate: datetime = None

    # Relationships
    datasetType: DatasetType
    datafiles: Annotated[list[Datafile], Len(min_length=1)]

    def excluded_dict(self) -> dict[str, Any]:
        """Utility function for excluding fields which should not be passed to ICAT for
        entity creation as kwargs.

        Returns:
            dict[str, Any]:
                Dictionary of fields, excluding None values and related Entities.
        """
        return self.dict(exclude={"datasetType", "datafiles"}, exclude_none=True)


class Investigation(BaseModel):
    name: str = Field(example="ABC123")
    visitId: str = Field(example="1")
    title: str = Field(example="Title")
    summary: str = Field(default=None, example="Summary")
    doi: str = Field(default=None, example="10.00000/00000")
    startDate: datetime = None
    endDate: datetime = None
    releaseDate: datetime = None

    # Relationships
    facility: Facility
    investigationType: InvestigationType
    instrument: Instrument
    facilityCycle: FacilityCycle
    datasets: Annotated[list[Dataset], Len(min_length=1)]

    @validator("releaseDate")
    def define_release_date(cls, v: datetime | None, values: dict) -> datetime:
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


class ArchiveRequest(BaseModel):
    investigations: Annotated[list[Investigation], Len(min_length=1)]


class ArchiveResponse(BaseModel):
    job_ids: list[str] = Field(example=["00000000-0000-0000-0000-000000000000"])
