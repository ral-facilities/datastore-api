from pydantic import BaseModel, Field, SecretStr


class Credentials(BaseModel):
    username: str = Field(examples=["root"])
    password: SecretStr = Field(examples=["pw"])


class LoginRequest(BaseModel):
    auth: str = Field(examples=["simple"])
    credentials: Credentials


class LoginResponse(BaseModel):
    sessionId: str = Field(examples=["00000000-0000-0000-0000-000000000000"])
