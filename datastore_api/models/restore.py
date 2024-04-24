from typing import Annotated

from annotated_types import Len
from pydantic import BaseModel, Field


class RestoreRequest(BaseModel):
    investigation_ids: Annotated[list[int], Len(min_length=1)]


class RestoreResponse(BaseModel):
    job_ids: list[str] = Field(example=["00000000-0000-0000-0000-000000000000"])
