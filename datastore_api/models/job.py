from enum import StrEnum

from pydantic import BaseModel


class JobState(StrEnum):
    staging = "STAGING"
    submitted = "SUBMITTED"
    active = "ACTIVE"
    finished = "FINISHED"
    finished_dirty = "FINISHEDDIRTY"
    failed = "FAILED"
    canceled = "CANCELED"


class TransferState(StrEnum):
    staging = "STAGING"
    on_hold_staging = "ON_HOLD_STAGING"
    started = "STARTED"
    submitted = "SUBMITTED"
    on_hold = "ON_HOLD"
    not_used = "NOT_USED"
    ready = "READY"
    active = "ACTIVE"
    archiving = "ARCHIVING"
    finished = "FINISHED"
    failed = "FAILED"
    canceled = "CANCELED"


class StatusResponse(BaseModel):
    status: dict  # TODO


class CompleteResponse(BaseModel):
    complete: bool


class PercentageResponse(BaseModel):
    percentage_complete: float


class CancelResponse(BaseModel):
    state: JobState
