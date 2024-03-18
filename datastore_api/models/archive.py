from datetime import datetime
from typing import Annotated

from annotated_types import Len
from pydantic import BaseModel, Field


class Facility(BaseModel):
    name: str = Field(example="facility")


class InvestigationType(BaseModel):
    name: str = Field(example="type")


class Instrument(BaseModel):
    name: str = Field(example="instrument")


class FacilityCycle(BaseModel):
    name: str = Field(example="20XX")


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
    cycle: FacilityCycle
    # TODO expand metadata


class ArchiveRequest(BaseModel):
    investigations: Annotated[list[Investigation], Len(min_length=1)]


class ArchiveResponse(BaseModel):
    job_id: str = Field(example="1")
