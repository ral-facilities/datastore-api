from uuid import UUID

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

security = HTTPBearer()


def validate_session_id(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> str:
    """Checks that a sessionId is in valid UUID4 format.

    Args:
        credentials (HTTPAuthorizationCredentials, optional):
            Credentials. Defaults to Depends(security).

    Raises:
        HTTPException: If the sessionId is not in the valid format

    Returns:
        str: The sessionId as a str.
    """
    session_id = credentials.credentials
    # check it's a UUID
    try:
        UUID(session_id, version=4)
    except ValueError as e:
        raise HTTPException(status_code=401, detail="value not a valid UUID") from e

    return session_id
