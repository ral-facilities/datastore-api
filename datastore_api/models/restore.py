from typing import Annotated

from annotated_types import Len
from pydantic import BaseModel, Field


class RestoreRequest(BaseModel):
    investigation_ids: Annotated[list[int], Len(min_length=1)]


class RestoreResponse(BaseModel):
    job_id: str = Field(example="1")
