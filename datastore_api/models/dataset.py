from pydantic import BaseModel, Field

from datastore_api.models.job import JobState, TransferState


class DatasetStatusResponse(BaseModel):
    state: JobState = Field(description="The overall state of the Dataset archival.")


class DatasetStatusListFilesResponse(DatasetStatusResponse):
    file_states: dict[str, TransferState] = Field(
        description="Mapping of individual Datafile locations to their archival state.",
    )
