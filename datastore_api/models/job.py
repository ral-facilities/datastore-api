from enum import StrEnum

from pydantic import BaseModel


class JobState(StrEnum):
    staging = "STAGING"
    submitted = "SUBMITTED"
    ready = "READY"
    active = "ACTIVE"
    archiving = "ARCHIVING"
    finished = "FINISHED"
    finished_dirty = "FINISHEDDIRTY"
    failed = "FAILED"
    canceled = "CANCELED"


COMPLETE_JOB_STATES = (
    JobState.finished,
    JobState.finished_dirty,
    JobState.failed,
    JobState.canceled,
)


class TransferState(StrEnum):
    new = "NEW"
    staging = "STAGING"
    started = "STARTED"
    submitted = "SUBMITTED"
    not_used = "NOT_USED"
    ready = "READY"
    active = "ACTIVE"
    archiving = "ARCHIVING"
    finished = "FINISHED"
    failed = "FAILED"
    canceled = "CANCELED"
    defunct = "DEFUNCT"


COMPLETE_TRANSFER_STATES = (
    TransferState.finished,
    TransferState.failed,
    TransferState.canceled,
    TransferState.defunct,
)


class StatusResponse(BaseModel):
    status: dict | list[dict]  # TODO


class CompleteResponse(BaseModel):
    complete: bool


class PercentageResponse(BaseModel):
    percentage_complete: float


class CancelResponse(BaseModel):
    state: JobState
