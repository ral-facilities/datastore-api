from pydantic import BaseModel, Field


class VersionResponse(BaseModel):
    version: str = Field(example="0.1.0")
