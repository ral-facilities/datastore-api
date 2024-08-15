from pydantic import BaseModel, Field

from datastore_api.models.icat import (
    Dataset,
    FacilityCycleIdentifier,
    FacilityIdentifier,
    InstrumentIdentifier,
    Investigation,
    InvestigationIdentifier,
)


class ArchiveRequest(BaseModel):
    facility_identifier: FacilityIdentifier
    instrument_identifier: InstrumentIdentifier
    facility_cycle_identifier: FacilityCycleIdentifier
    investigation_identifier: Investigation | InvestigationIdentifier
    dataset: Dataset


class ArchiveResponse(BaseModel):
    job_ids: list[str] = Field(example=["00000000-0000-0000-0000-000000000000"])
