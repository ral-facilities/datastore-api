from pydantic import BaseModel, Field, root_validator


class RestoreRequest(BaseModel):
    investigation_ids: list[int] = []
    dataset_ids: list[int] = []
    datafile_ids: list[int] = []

    @root_validator()
    def validate_ids(cls, values: dict) -> dict:
        investigations = len(values.get("investigation_ids", []))
        datasets = len(values.get("dataset_ids", []))
        datafiles = len(values.get("datafile_ids", []))
        if investigations + datasets + datafiles == 0:
            raise ValueError("At least one id must be provided")

        return values


class RestoreResponse(BaseModel):
    job_ids: list[str] = Field(example=["00000000-0000-0000-0000-000000000000"])
