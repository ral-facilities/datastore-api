from pydantic import BaseModel, Field

from datastore_api.models.icat import (
    Dataset,
    Investigation,
    InvestigationIdentifier,
)


class ArchiveRequest(BaseModel):
    investigation_identifier: Investigation | InvestigationIdentifier
    dataset: Dataset


class ArchiveResponse(BaseModel):
    dataset_ids: list[int] = Field(
        description="ICAT Dataset Entity ids",
        examples=[[1]],
    )
    job_ids: list[str] = Field(examples=[["00000000-0000-0000-0000-000000000000"]])
