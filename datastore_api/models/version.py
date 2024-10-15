from pydantic import BaseModel, Field


class VersionResponse(BaseModel):
    version: str = Field(examples=["0.1.0"])
