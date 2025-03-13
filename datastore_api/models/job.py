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

ACTIVE_JOB_STATES = (
    JobState.staging,
    JobState.submitted,
    JobState.ready,
    JobState.active,
    JobState.archiving,
)

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

ACTIVE_TRANSFER_STATES = (
    TransferState.new,
    TransferState.staging,
    TransferState.started,
    TransferState.submitted,
    TransferState.not_used,
    TransferState.ready,
    TransferState.active,
    TransferState.archiving,
)

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
