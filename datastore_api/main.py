from functools import lru_cache
from importlib import metadata

from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware
import fts3.rest.client.easy as fts3
from typing_extensions import Annotated

from datastore_api.auth import validate_session_id
from datastore_api.config import Settings
from datastore_api.icat_client import IcatClient
from datastore_api.models.archive import ArchiveRequest, ArchiveResponse
from datastore_api.models.job import CancelResponse, StatusResponse
from datastore_api.models.login import LoginRequest, LoginResponse
from datastore_api.models.restore import RestoreRequest, RestoreResponse
from datastore_api.models.version import VersionResponse


app = FastAPI(
    title="Datastore API",
    description="""
The Datastore API accepts requests for the archival or retrieval of experimental data.
These trigger subsequent requests to create corresponding metadata in ICAT,
and schedules the transfer of the data using FTS3.""",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@lru_cache
def get_settings() -> Settings:
    """Get and cache the API settings to prevent overhead from reading from file.

    Returns:
        Settings: The configurations settings for the API.
    """
    return Settings()


@lru_cache
def get_icat_client() -> IcatClient:
    """Initialise and cache client for making calls to ICAT.

    Returns:
        IcatClient: Wrapper for calls to ICAT.
    """
    settings = get_settings()
    return IcatClient(settings)


@lru_cache
def get_fts3_context() -> fts3.Context:
    """Initialise and cache the context for making calls to FTS.

    Returns:
        fts3.Context: Context for calls to FTS.
    """
    settings = get_settings()
    return fts3.Context(endpoint=settings.fts3.endpoint)


@app.post(
    "/login",
    response_description="An ICAT sessionId",
    summary=(
        "Using the provided credentials authenticates against ICAT and returns the "
        "sessionId"
    ),
    tags=["Login"],
)
def login(
    login_request: LoginRequest,
    icat_client: Annotated[IcatClient, Depends(get_icat_client)],
) -> LoginResponse:
    """Using the provided credentials authenticates against ICAT and returns the
    sessionId.
    \f
    Args:
        login_request (LoginRequest): Request body containing the user's credentials.
        icat_client (IcatClient): Cached client for calls to ICAT.

    Returns:
        LoginResponse: An ICAT sessionId.
    """
    return LoginResponse(sessionId=icat_client.login(login_request=login_request))


@app.post(
    "/archive",
    response_description="The FTS job id for the requested transfer",
    summary=(
        "Submit a request to archive experimental data, "
        "recording metadata in ICAT and creating an FTS transfer"
    ),
    tags=["Archive"],
)
def archive(
    archive_request: ArchiveRequest,
    session_id: Annotated[str, Depends(validate_session_id)],
    icat_client: Annotated[IcatClient, Depends(get_icat_client)],
    fts3_context: Annotated[fts3.Context, Depends(get_fts3_context)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> ArchiveResponse:
    """Submit a request to archive experimental data, recording metadata in ICAT and
    creating an FTS transfer.
    \f
    Args:
        archive_request (ArchiveRequest): Metadata for the entities to be archived.
        session_id (str): ICAT sessionId.
        icat_client (IcatClient): Cached client for calls to ICAT.
        fts3_context (fts3.Context): Cached context for calls to FTS.
        settings (Settings): Cached API configuration settings.

    Returns:
        ArchiveResponse: FTS job_id for archive transfer.
    """
    paths = icat_client.create_investigations(
        session_id=session_id,
        investigations=archive_request.investigations,
    )
    transfers = []
    for path in paths:
        source = f"{settings.fts3.instrument_data_cache}/{path}"
        alternate_source = f"{settings.fts3.user_data_cache}/{path}"
        destination = f"{settings.fts3.tape_archive}/{path}"
        transfer = fts3.new_transfer(source=source, destination=destination)
        transfer["sources"].append(alternate_source)
        transfers.append(transfer)
    job = fts3.new_job(transfers=transfers)
    return ArchiveResponse(job_id=fts3.submit(context=fts3_context, job=job))


@app.post(
    "/restore",
    response_description="The FTS job id for the requested transfer",
    summary="Submit a request to restore experimental data, creating an FTS transfer",
    tags=["Restore"],
)
def restore(
    restore_request: RestoreRequest,
    session_id: Annotated[str, Depends(validate_session_id)],
    icat_client: Annotated[IcatClient, Depends(get_icat_client)],
    fts3_context: Annotated[fts3.Context, Depends(get_fts3_context)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> RestoreResponse:
    """Submit a request to restore experimental data, creating an FTS transfer.
    \f
    Args:
        restore_request (RestoreRequest): ICAT ids for Investigations to restore.
        session_id (str): ICAT sessionId.
        icat_client (IcatClient): Cached client for calls to ICAT.
        fts3_context (fts3.Context): Cached context for calls to FTS.
        settings (Settings): Cached API configuration settings.

    Returns:
        RestoreResponse: FTS job_id for restore transfer.
    """
    paths = icat_client.get_investigation_paths(
        session_id=session_id,
        investigation_ids=restore_request.investigation_ids,
    )
    transfers = []
    for path in paths:
        transfer = fts3.new_transfer(
            source=f"{settings.fts3.tape_archive}/{path}",
            destination=f"{settings.fts3.user_data_cache}/{path}",
        )
        transfers.append(transfer)
    job = fts3.new_job(
        transfers=transfers,
        bring_online=settings.fts3.bring_online,
        copy_pin_lifetime=settings.fts3.copy_pin_lifetime,
    )
    return RestoreResponse(job_id=fts3.submit(context=fts3_context, job=job))


@app.delete(
    "/job/{job_id}",
    response_description="The terminal state of the canceled job",
    summary="Cancel a job previously submitted to FTS",
    tags=["Job"],
)
def cancel(
    job_id: str,
    session_id: Annotated[str, Depends(validate_session_id)],
    icat_client: Annotated[IcatClient, Depends(get_icat_client)],
    fts3_context: Annotated[fts3.Context, Depends(get_fts3_context)],
) -> CancelResponse:
    """Cancel a job previously submitted to FTS.
    \f
    Args:
        job_id (str): FTS id for a submitted job.
        session_id (str): ICAT sessionId.
        icat_client (IcatClient): Cached client for calls to ICAT.
        fts3_context (fts3.Context): Cached context for calls to FTS.

    Returns:
        CancelResponse: Terminal state of the canceled job.
    """
    icat_client.authorise_admin(session_id=session_id)
    state = fts3.cancel(context=fts3_context, job_id=job_id)
    return CancelResponse(state=state)


@app.get(
    "/job/{job_id}",
    response_description="JSON describing the status of the requested job",
    summary="Get details of a job previously submitted to FTS",
    tags=["Job"],
)
def status(
    job_id: str,
    session_id: Annotated[str, Depends(validate_session_id)],
    icat_client: Annotated[IcatClient, Depends(get_icat_client)],
    fts3_context: Annotated[fts3.Context, Depends(get_fts3_context)],
) -> StatusResponse:
    """Get details of a job previously submitted to FTS.
    \f
    Args:
        job_id (str): FTS id for a submitted job.
        session_id (str): ICAT sessionId.
        icat_client (IcatClient): Cached client for calls to ICAT.
        fts3_context (fts3.Context): Cached context for calls to FTS.

    Returns:
        StatusResponse: Details of the requested job.
    """
    icat_client.authorise_admin(session_id=session_id)
    status = fts3.get_job_status(context=fts3_context, job_id=job_id)
    return StatusResponse(status=status)


@app.get(
    "/version",
    response_description="The current version of the API",
    summary="Get the version of the API",
    tags=["Version"],
)
def version() -> VersionResponse:
    """Get the version of the API.
    \f
    Returns:
        VersionResponse: Version of the API.
    """
    return VersionResponse(version=metadata.version("datastore-api"))
