from pydantic import BaseModel


class StatusResponse(BaseModel):
    status: dict  # TODO


class CancelResponse(BaseModel):
    state: str  # TODO
