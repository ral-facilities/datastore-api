from importlib import metadata
from typing import Annotated

from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware

from datastore_api.auth import validate_session_id
from datastore_api.fts3_client import Fts3Client, get_fts3_client
from datastore_api.icat_client import IcatClient
from datastore_api.investigation_archiver import InvestigationArchiver
from datastore_api.lifespan import lifespan
from datastore_api.models.archive import ArchiveRequest, ArchiveResponse
from datastore_api.models.job import (
    CancelResponse,
    CompleteResponse,
    JobState,
    PercentageResponse,
    StatusResponse,
    TransferState,
)
from datastore_api.models.login import LoginRequest, LoginResponse
from datastore_api.models.restore import RestoreRequest, RestoreResponse
from datastore_api.models.version import VersionResponse
from datastore_api.transfer_controller import RestoreController


app = FastAPI(
    title="Datastore API",
    description="""
The Datastore API accepts requests for the archival or retrieval of experimental data.
These trigger subsequent requests to create corresponding metadata in ICAT,
and schedules the transfer of the data using FTS3.""",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


SessionIdDependency = Annotated[str, Depends(validate_session_id)]
Fts3ClientDependency = Annotated[Fts3Client, Depends(get_fts3_client)]


@app.post(
    "/login",
    response_description="An ICAT sessionId",
    summary=(
        "Using the provided credentials authenticates against ICAT and returns the "
        "sessionId"
    ),
    tags=["Login"],
)
def login(login_request: LoginRequest) -> LoginResponse:
    """Using the provided credentials authenticates against ICAT and returns the
    sessionId.
    \f
    Args:
        login_request (LoginRequest): Request body containing the user's credentials.

    Returns:
        LoginResponse: An ICAT sessionId.
    """
    return LoginResponse(sessionId=IcatClient().login(login_request=login_request))


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
    session_id: SessionIdDependency,
    fts3_client: Fts3ClientDependency,
) -> ArchiveResponse:
    """Submit a request to archive experimental data, recording metadata in ICAT and
    creating an FTS transfer.
    \f
    Args:
        archive_request (ArchiveRequest): Metadata for the entities to be archived.
        session_id (str): ICAT sessionId.
        fts3_client (Fts3Client): Cached client for calls to FTS.

    Returns:
        ArchiveResponse: FTS job_id for archive transfer.
    """
    icat_client = IcatClient(session_id=session_id)
    icat_client.authorise_admin()
    beans = []
    job_ids = []
    for investigation in archive_request.investigations:
        investigation_archiver = InvestigationArchiver(
            icat_client=icat_client,
            fts3_client=fts3_client,
            investigation=investigation,
        )
        investigation_archiver.archive_datasets()
        beans.extend(investigation_archiver.beans)
        job_ids.extend(investigation_archiver.job_ids)

    icat_client.create_many(beans=beans)

    return ArchiveResponse(job_ids=investigation_archiver.job_ids)


@app.post(
    "/restore",
    response_description="The FTS job id for the requested transfer",
    summary="Submit a request to restore experimental data, creating an FTS transfer",
    tags=["Restore"],
)
def restore(
    restore_request: RestoreRequest,
    session_id: SessionIdDependency,
    fts3_client: Fts3ClientDependency,
) -> RestoreResponse:
    """Submit a request to restore experimental data, creating an FTS transfer.
    \f
    Args:
        restore_request (RestoreRequest): ICAT ids for Investigations to restore.
        session_id (str): ICAT sessionId.
        fts3_client (Fts3Client): Cached client for calls to FTS.

    Returns:
        RestoreResponse: FTS job_id for restore transfer.
    """
    icat_client = IcatClient(session_id=session_id)
    paths = icat_client.get_paths(
        investigation_ids=restore_request.investigation_ids,
        dataset_ids=restore_request.dataset_ids,
        datafile_ids=restore_request.datafile_ids,
    )
    restore_controller = RestoreController(fts3_client=fts3_client, paths=paths)
    restore_controller.create_fts_jobs()
    return RestoreResponse(job_ids=restore_controller.job_ids)


@app.delete(
    "/job/{job_id}",
    response_description="The terminal state of the canceled job",
    summary="Cancel a job previously submitted to FTS",
    tags=["Job"],
)
def cancel(job_id: str, fts3_client: Fts3ClientDependency) -> CancelResponse:
    """Cancel a job previously submitted to FTS.
    \f
    Args:
        job_id (str): FTS id for a submitted job.
        fts3_client (Fts3Client): Cached client for calls to FTS.

    Returns:
        CancelResponse: Terminal state of the canceled job.
    """
    icat_client = IcatClient()
    icat_client.login_functional()
    icat_client.check_job_id(job_id=job_id)
    state = fts3_client.cancel(job_id=job_id)
    return CancelResponse(state=state)


@app.get(
    "/job/{job_id}",
    response_description="JSON describing the status of the requested job",
    summary="Get details of a job previously submitted to FTS",
    tags=["Job"],
)
def status(job_id: str, fts3_client: Fts3ClientDependency) -> StatusResponse:
    """Get details of a job previously submitted to FTS.
    \f
    Args:
        job_id (str): FTS id for a submitted job.
        fts3_client (Fts3Client): Cached client for calls to FTS.

    Returns:
        StatusResponse: Details of the requested job.
    """
    status = fts3_client.status(job_id=job_id)
    return StatusResponse(status=status)


@app.get(
    "/job/{job_id}/complete",
    response_description="Whether the job is complete",
    summary="Whether the job ended in the FINISHED, FINISHEDDIRTY or FAILED states",
    tags=["Job"],
)
def complete(job_id: str, fts3_client: Fts3ClientDependency) -> CompleteResponse:
    """Whether the job ended in the FINISHED, FINISHEDDIRTY or FAILED states.
    \f
    Args:
        job_id (str): FTS id for a submitted job.
        fts3_context (fts3.Context): Cached context for calls to FTS.

    Returns:
        CompleteResponse: Completeness of the requested job.
    """
    status = fts3_client.status(job_id=job_id)
    complete_states = (JobState.finished, JobState.finished_dirty, JobState.failed)
    return CompleteResponse(complete=status["job_state"] in complete_states)


@app.get(
    "/job/{job_id}/percentage",
    response_description="Percentage of individual transfers that are completed",
    summary="Percentage of individual transfers that are completed",
    tags=["Job"],
)
def percentage(job_id: str, fts3_client: Fts3ClientDependency) -> PercentageResponse:
    """Percentage of individual transfers that are completed.
    \f
    Args:
        job_id (str): FTS id for a submitted job.
        fts3_context (fts3.Context): Cached context for calls to FTS.

    Returns:
        PercentageResponse: Percentage of individual transfers that are completed.
    """
    files_complete = 0
    status = fts3_client.status(job_id=job_id)
    files_total = len(status["files"])
    for file in status["files"]:
        if file["file_state"] in (TransferState.finished, TransferState.failed):
            files_complete += 1

    return PercentageResponse(percentage_complete=100 * files_complete / files_total)


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
