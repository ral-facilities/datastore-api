from pydantic import BaseModel, Field


class Credentials(BaseModel):
    username: str = Field(example="root")
    password: str = Field(example="pw")


class LoginRequest(BaseModel):
    auth: str = Field(example="simple")
    credentials: Credentials


class LoginResponse(BaseModel):
    sessionId: str = Field(example="00000000-0000-0000-0000-000000000000")
