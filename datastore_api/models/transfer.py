from enum import StrEnum

from pydantic import BaseModel, Field, model_validator


class BucketAcl(StrEnum):
    PRIVATE = "private"
    PUBLIC_READ = "public-read"


class TransferRequest(BaseModel):
    investigation_ids: set[int] = set()
    dataset_ids: set[int] = set()
    datafile_ids: set[int] = set()

    @model_validator(mode="after")
    def validate_ids(self) -> "TransferRequest":
        investigations = len(self.investigation_ids)
        datasets = len(self.dataset_ids)
        datafiles = len(self.datafile_ids)
        if investigations + datasets + datafiles == 0:
            raise ValueError("At least one id must be provided")

        return self


class TransferS3Request(TransferRequest):
    bucket_acl: BucketAcl = BucketAcl.PRIVATE


class TransferResponse(BaseModel):
    job_ids: list[str] = Field(examples=[["00000000-0000-0000-0000-000000000000"]])


class TransferS3Response(TransferResponse):
    bucket_name: str
