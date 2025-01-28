from importlib import metadata
import logging
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from datastore_api.auth import validate_session_id
from datastore_api.clients.fts3_client import Fts3Client, get_fts3_client
from datastore_api.clients.icat_client import IcatClient
from datastore_api.config import get_settings, Storage, StorageType
from datastore_api.controllers.bucket_controller import BucketController
from datastore_api.controllers.investigation_archiver import InvestigationArchiver
from datastore_api.controllers.state_controller import StateController
from datastore_api.controllers.state_counter import StateCounter
from datastore_api.controllers.transfer_controller import (
    DatasetReArchiver,
    TransferController,
)
from datastore_api.lifespan import lifespan
from datastore_api.models.archive import ArchiveRequest, ArchiveResponse
from datastore_api.models.dataset import (
    DatasetStatusListFilesResponse,
    DatasetStatusResponse,
)
from datastore_api.models.job import (
    CancelResponse,
    COMPLETE_JOB_STATES,
    CompleteResponse,
    PercentageResponse,
    StatusResponse,
)
from datastore_api.models.login import LoginRequest, LoginResponse
from datastore_api.models.transfer import (
    TransferRequest,
    TransferResponse,
    TransferS3Request,
    TransferS3Response,
)
from datastore_api.models.version import VersionResponse

LOGGER = logging.getLogger(__name__)


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


def _get_storage(key: str) -> Storage:
    """Get representation of storage from the FTS3 settings.

    Args:
        key (str): Storage key.

    Returns:
        Storage: Corresponding Pydantic object.
    """
    settings = get_settings()
    try:
        return settings.fts3.storage_endpoints[key]
    except KeyError as e:
        keys = settings.fts3.storage_endpoints.keys()
        detail = f"{key} is not a recognised storage key: {set(keys)}"
        raise HTTPException(422, detail) from e


def validate_source_key(source_key: str) -> str:
    """Implicitly raise KeyError if source_key unknown.

    Args:
        source_key (str): Key that should be present in the FTS3 storage endpoints.

    Returns:
        str: source_key
    """
    _get_storage(key=source_key)
    return source_key


def validate_destination_key(destination_key: str) -> str:
    """Implicitly raise KeyError if destination_key unknown.

    Args:
        destination_key (str): Key that should be present in the FTS3 storage endpoints.

    Returns:
        str: destination_key
    """
    _get_storage(key=destination_key)
    return destination_key


def validate_s3_storage_key(s3_storage_key: str) -> str:
    """Implicitly raise KeyError if s3_storage_key unknown.

    Args:
        s3_storage_key (str):
            Key that should be present in the FTS3 storage endpoints and have S3 type.

    Raises:
        ValueError: If not S3 type

    Returns:
        str: s3_storage_key
    """
    storage = _get_storage(key=s3_storage_key)
    if storage.storage_type != StorageType.S3:
        detail = f"{s3_storage_key} is {storage.storage_type}, not S3 storage"
        raise HTTPException(422, detail)

    return s3_storage_key


def validate_archive_storage() -> None:
    settings = get_settings()
    if settings.fts3.archive_endpoint is None:
        detail = "Archive functionality not implemented for this instance"
        raise HTTPException(501, detail)


SessionIdDependency = Annotated[str, Depends(validate_session_id)]
Fts3ClientDependency = Annotated[Fts3Client, Depends(get_fts3_client)]
SourceKey = Annotated[str, Depends(validate_source_key)]
DestinationKey = Annotated[str, Depends(validate_destination_key)]
S3StorageKey = Annotated[str, Depends(validate_s3_storage_key)]


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
    "/archive/{source_key}",
    response_description="The FTS job id for the requested transfer",
    summary=(
        "Submit a request to archive experimental data, "
        "recording metadata in ICAT and creating an FTS transfer"
    ),
    tags=["Archive"],
)
def archive(
    source_key: SourceKey,
    archive_request: ArchiveRequest,
    session_id: SessionIdDependency,
) -> ArchiveResponse:
    """Submit a request to archive experimental data, recording metadata in ICAT and
    creating an FTS transfer.
    \f
    Args:
        source_key (SourceKey):
            Key identifying the storage to use as the transfer source.
        archive_request (ArchiveRequest): Metadata for the entities to be archived.
        session_id (str): ICAT sessionId.

    Returns:
        ArchiveResponse: FTS job_id for archive transfer.
    """
    validate_archive_storage()
    icat_client = IcatClient(session_id=session_id)
    icat_client.authorise_admin()
    investigation_archiver = InvestigationArchiver(
        icat_client=icat_client,
        source_key=source_key,
        investigation=archive_request.investigation_identifier,
        datasets=[archive_request.dataset],
    )
    investigation_archiver.archive_datasets()

    icat_ids = icat_client.create_many(beans=investigation_archiver.beans)

    LOGGER.info(
        "Submitted FTS archival jobs for %s transfers with ids %s",
        investigation_archiver.total_transfers,
        investigation_archiver.job_ids,
    )

    return ArchiveResponse(
        dataset_ids=list(icat_ids),
        job_ids=investigation_archiver.job_ids,
    )


@app.post(
    "/restore/{destination_key}",
    response_description="The FTS job id for the requested transfer",
    summary=(
        "Submit a request to restore experimental data to another location, "
        "creating an FTS transfer"
    ),
    tags=["Restore"],
)
def restore(
    destination_key: DestinationKey,
    transfer_request: TransferS3Request | TransferRequest,
    session_id: SessionIdDependency,
) -> TransferS3Response | TransferResponse:
    """Submit a request to restore experimental data to the another location,
    creating an FTS transfer.
    \f
    Args:
        destination_key (DestinationKey):
            Key identifying the storage to use as the transfer destination.
        transfer_request (TransferS3Request | TransferRequest):
            ICAT ids for Investigations to restore.
        session_id (str): ICAT sessionId.

    Returns:
        TransferS3Response | TransferResponse: FTS job_id for restore transfer.
    """
    validate_archive_storage()
    icat_client = IcatClient(session_id=session_id)
    datafile_entities = icat_client.get_unique_datafiles(
        investigation_ids=transfer_request.investigation_ids,
        dataset_ids=transfer_request.dataset_ids,
        datafile_ids=transfer_request.datafile_ids,
    )
    is_s3_request = isinstance(transfer_request, TransferS3Request)
    restore_controller = TransferController(
        datafile_entities=datafile_entities,
        destination_key=destination_key,
        bucket_acl=transfer_request.bucket_acl if is_s3_request else None,
    )
    response = restore_controller.create_fts_jobs()

    message = "Submitted FTS restore jobs for %s transfers with ids %s"
    LOGGER.info(message, restore_controller.total_transfers, restore_controller.job_ids)

    return response


@app.post(
    "/transfer/{source_key}/{destination_key}",
    response_description="The FTS job id for the requested transfer",
    summary=(
        "Submit a request to restore experimental data to another location, "
        "creating an FTS transfer"
    ),
    tags=["Restore"],
)
def transfer(
    source_key: SourceKey,
    destination_key: DestinationKey,
    transfer_request: TransferS3Request | TransferRequest,
    session_id: SessionIdDependency,
) -> TransferS3Response | TransferResponse:
    """Submit a request to transfer experimental data to the another location,
    creating an FTS transfer.
    \f
    Args:
        source_key (SourceKey):
            Key identifying the storage to use as the transfer source.
        destination_key (DestinationKey):
            Key identifying the storage to use as the transfer destination.
        restore_request (TransferS3Request | TransferRequest):
            ICAT ids for Investigations to restore.
        session_id (str): ICAT sessionId.

    Returns:
        TransferS3Response | TransferResponse: FTS job_id for restore transfer.
    """
    icat_client = IcatClient(session_id=session_id)
    datafile_entities = icat_client.get_unique_datafiles(
        investigation_ids=transfer_request.investigation_ids,
        dataset_ids=transfer_request.dataset_ids,
        datafile_ids=transfer_request.datafile_ids,
    )
    is_s3_request = isinstance(transfer_request, TransferS3Request)
    restore_controller = TransferController(
        datafile_entities=datafile_entities,
        source_key=source_key,
        destination_key=destination_key,
        bucket_acl=transfer_request.bucket_acl if is_s3_request else None,
    )
    response = restore_controller.create_fts_jobs()

    message = "Submitted FTS transfer jobs for %s transfers with ids %s"
    LOGGER.info(message, restore_controller.total_transfers, restore_controller.job_ids)

    return response


@app.get(
    "/bucket/{s3_storage_key}/{bucket_name}",
    response_description="The URL to download the data",
    summary="Get the download link for the records in the download cache",
    tags=["Bucket"],
)
def get_bucket_data(
    s3_storage_key: S3StorageKey,
    bucket_name: str,
    expiration: int | None = None,
) -> dict[str, str]:
    """Get the download links for the records in the download cache
    \f
    Args:
        s3_storage_key (S3StorageKey):
            Key identifying the storage where the bucket is located.
        bucket_name (str): The bucket containing data to download.
        expiration (int): Expiration date of the download url in seconds.

    Returns:
        dict[str, str]: Dictionary with generated presigned urls.
    """
    bucket_controller = BucketController(storage_key=s3_storage_key, name=bucket_name)
    return bucket_controller.get_data(expiration=expiration)


@app.get(
    "/bucket/{s3_storage_key}/{bucket_name}/status",
    response_description="List of fts3 job statuses relating to the specified bucket",
    summary="Get details of FTS jobs relating to the specified bucket",
    tags=["Bucket"],
)
def get_bucket_status(
    s3_storage_key: S3StorageKey,
    bucket_name: str,
    fts3_client: Fts3ClientDependency,
) -> StatusResponse:
    """Get details of FTS jobs relating to the specified bucket
    \f
    Args:
        s3_storage_key (S3StorageKey):
            Key identifying the storage where the bucket is located.
        bucket_name (str): Name of the bucket from which to retrieve job statuses.
        fts3_client (Fts3Client): Cached client for calls to FTS.

    Returns:
        StatusResponse: List of job statuses relating to the specified bucket.
    """
    bucket_controller = BucketController(storage_key=s3_storage_key, name=bucket_name)
    job_ids = bucket_controller.cached_job_states
    statuses = fts3_client.statuses(job_ids=job_ids, list_files=True)
    bucket_controller.update_job_ids(statuses=statuses, check_files=False)
    return StatusResponse(status=statuses)


@app.get(
    "/bucket/{s3_storage_key}/{bucket_name}/complete",
    response_description="Completeness of jobs relating to the specified bucket.",
    summary="Whether all jobs relating to the bucket are complete.",
    tags=["Bucket"],
)
def get_bucket_complete(
    s3_storage_key: S3StorageKey,
    bucket_name: str,
) -> CompleteResponse:
    """Whether all jobs relating to the bucket are complete.

    Args:
        s3_storage_key (S3StorageKey):
            Key identifying the storage where the bucket is located.
        bucket_name (str): Name of the bucket from which to retrieve job statuses.

    Returns:
        CompleteResponse: Completeness of jobs relating to the specified bucket.
    """
    bucket_controller = BucketController(storage_key=s3_storage_key, name=bucket_name)
    return CompleteResponse(complete=bucket_controller.complete)


@app.get(
    "/bucket/{s3_storage_key}/{bucket_name}/percentage",
    response_description="Percentage of all individual transfers to the bucket",
    summary="Percentage of all individual transfers to the bucket, that are completed",
    tags=["Bucket"],
)
def get_bucket_percentage(
    bucket_name: str,
    s3_storage_key: S3StorageKey,
    fts3_client: Fts3ClientDependency,
) -> PercentageResponse:
    """Percentage of all individual transfers to the bucket, that are completed

    Args:
        s3_storage_key (S3StorageKey):
            Key identifying the storage where the bucket is located.
        bucket_name (str): Name of the bucket for which the percentage is being checked.
        fts3_client (Fts3Client): Cached client for calls to FTS.

    Returns:
        PercentageResponse: Percentage of all individual transfers to the bucket
    """
    bucket_controller = BucketController(storage_key=s3_storage_key, name=bucket_name)
    job_ids = bucket_controller.cached_job_states
    statuses = fts3_client.statuses(job_ids=job_ids, list_files=True)
    state_counter = bucket_controller.update_job_ids(
        statuses=statuses,
        check_files=True,
    )
    return PercentageResponse(percentage_complete=state_counter.file_percentage)


@app.delete(
    "/bucket/{s3_storage_key}/{bucket_name}",
    response_description="None",
    summary="Delete a specified bucket",
    tags=["Bucket"],
)
def delete_bucket(s3_storage_key: S3StorageKey, bucket_name: str) -> None:
    """Delete an S3 bucket with its content
    \f
    Args:
        s3_storage_key (S3StorageKey):
            Key identifying the storage where the bucket is located.
        bucket_name (str): Name of the bucket to delete
    """
    bucket_controller = BucketController(storage_key=s3_storage_key, name=bucket_name)
    bucket_controller.delete()


@app.put(
    "/dataset/{dataset_id}/retry/{source_key}",
    response_description="The FTS job id(s) for the requested transfer(s)",
    summary="Retry the transfer of the requested Dataset",
    tags=["Dataset"],
)
def put_dataset(
    session_id: SessionIdDependency,
    source_key: SourceKey,
    dataset_id: str,
) -> ArchiveResponse:
    """Get details of a previously archived Dataset
    \f
    Args:
        dataset_id (str): ICAT Dataset id.
        source_key (SourceKey):
            Key identifying the storage to use as the transfer source.
        session_id (SessionIdDependency): ICAT sessionId.

    Returns:
        ArchiveResponse: FTS job_id for archive transfer.
    """
    icat_client = IcatClient(session_id=session_id)
    icat_client.authorise_admin()
    state_controller = StateController(session_id=session_id)
    status: DatasetStatusListFilesResponse = state_controller.get_dataset_status(
        dataset_id=dataset_id,
        list_files=True,
    )
    archiver = DatasetReArchiver(
        icat_client=icat_client,
        dataset_id=dataset_id,
        status=status,
        source_key=source_key,
    )
    archiver.create_fts_jobs()
    return ArchiveResponse(dataset_ids=[dataset_id], job_ids=archiver.job_ids)


@app.get(
    "/dataset/{dataset_id}/status",
    response_description="JSON describing the status of the requested Dataset",
    summary="Get details of a previously archived Dataset",
    tags=["Dataset"],
)
def dataset_status(
    session_id: SessionIdDependency,
    dataset_id: str,
    list_files: bool = True,
) -> DatasetStatusResponse:
    """Get details of a previously archived Dataset
    \f
    Args:
        session_id (SessionIdDependency): ICAT sessionId.
        dataset_id (str): ICAT Dataset id.
        list_files (bool, optional): Include details of Datafiles. Defaults to True.

    Returns:
        DatasetStatusResponse: Details of the Dataset (and Datafile) state(s).
    """
    state_controller = StateController(session_id=session_id)
    return state_controller.get_dataset_status(
        dataset_id=dataset_id,
        list_files=list_files,
    )


@app.put(
    "/dataset/{dataset_id}/status",
    summary="Explicitly set the state of an archived Dataset and its Datafiles",
    tags=["Dataset"],
)
def put_dataset_status(
    session_id: SessionIdDependency,
    dataset_id: str,
    new_state: str = "UNKNOWN",
    set_deletion_date: bool = False,
) -> None:
    """Explicitly set the state of an archived Dataset and its Datafiles
    \f
    Args:
        session_id (SessionIdDependency): ICAT sessionId.
        dataset_id (str): ICAT Dataset id.
        new_state (str, optional):
            New state with which to mark the Dataset and Datafiles.
            Defaults to "UNKNOWN".
        set_deletion_date (bool, optional):
            Whether to set the deletion date parameter to the current datetime.
            Defaults to False.
    """
    state_controller = StateController(session_id=session_id)
    state_controller.set_dataset_state(
        dataset_id=dataset_id,
        new_state=new_state,
        set_deletion_date=set_deletion_date,
    )
    state_controller.set_datafile_states(
        dataset_id=dataset_id,
        new_state=new_state,
        set_deletion_date=set_deletion_date,
    )


@app.get(
    "/dataset/{dataset_id}/complete",
    response_description="Whether the archival of the Dataset is complete",
    summary=(
        "Whether the jobs for the Dataset ended in the FINISHED, FINISHEDDIRTY or "
        "FAILED states"
    ),
    tags=["Dataset"],
)
def dataset_complete(
    session_id: SessionIdDependency,
    dataset_id: str,
) -> CompleteResponse:
    """Get details of a previously archived Dataset
    \f
    Args:
        session_id (SessionIdDependency): ICAT sessionId.
        dataset_id (str): ICAT Dataset id.

    Returns:
        CompleteResponse: Details of the Dataset (and Datafile) state(s).
    """
    state_controller = StateController(session_id=session_id)
    status = state_controller.get_dataset_status(dataset_id=dataset_id)
    return CompleteResponse(complete=status.state in COMPLETE_JOB_STATES)


@app.get(
    "/dataset/{dataset_id}/percentage",
    response_description=(
        "Percentage of individual transfers that have completed for the requested "
        "Dataset"
    ),
    summary=(
        "Percentage of individual transfers that have completed for the requested "
        "Dataset"
    ),
    tags=["Dataset"],
)
def dataset_percentage(
    session_id: SessionIdDependency,
    dataset_id: str,
) -> PercentageResponse:
    """Percentage of individual transfers that have completed for the requested Dataset.
    \f
    Args:
        session_id (SessionIdDependency): ICAT sessionId.
        dataset_id (str): ICAT Dataset id.

    Returns:
        PercentageResponse: Percentage of individual transfers that are completed.
    """
    state_controller = StateController(session_id=session_id)
    status: DatasetStatusListFilesResponse = state_controller.get_dataset_status(
        dataset_id=dataset_id,
        list_files=True,
    )
    files_total = len(status.file_states)
    files_complete = StateController.sum_completed_transfers(status.file_states)
    return PercentageResponse(percentage_complete=100 * files_complete / files_total)


@app.put(
    "/datafile/{datafile_id}/status",
    summary="Explicitly set the state of an archived Datafile",
    tags=["Datafile"],
)
def put_datafile_status(
    session_id: SessionIdDependency,
    datafile_id: str,
    new_state: str = "UNKNOWN",
    set_deletion_date: bool = False,
) -> None:
    """Explicitly set the state of an archived Datafile.
    \f
    Args:
        session_id (SessionIdDependency): ICAT sessionId.
        datafile_id (str): ICAT Datafile id.
        new_state (str, optional):
            New state with which to mark the Datafile.
            Defaults to "UNKNOWN".
        set_deletion_date (bool, optional):
            Whether to set the deletion date parameter to the current datetime.
            Defaults to False.
    """
    state_controller = StateController(session_id=session_id)
    state_controller.set_datafile_state(
        datafile_id=datafile_id,
        new_state=new_state,
        set_deletion_date=set_deletion_date,
    )


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
    "/job/{job_id}/status",
    response_description="JSON describing the status of the requested job",
    summary="Get details of a job previously submitted to FTS",
    tags=["Job"],
)
def status(
    fts3_client: Fts3ClientDependency,
    job_id: str,
    list_files: bool = True,
    verbose: bool = True,
) -> StatusResponse | DatasetStatusResponse | DatasetStatusListFilesResponse:
    """Get details of a job previously submitted to FTS.
    \f
    Args:
        fts3_client (Fts3Client): Cached client for calls to FTS.
        job_id (str): FTS id for a submitted job.
        list_files (bool, optional): Include details of Datafiles. Defaults to True.

    Returns:
        StatusResponse: Details of the requested job.
    """
    status = fts3_client.status(job_id=job_id, list_files=list_files)

    if verbose:  # verbose = True
        return StatusResponse(status=status)
    else:  # verbose = False
        if list_files:  # list_files = True
            file_states = {}
            for file_status in status["files"]:
                file_path, file_state = StateCounter.get_state(file_status=file_status)
                file_states[file_path] = file_state
            return DatasetStatusListFilesResponse(
                state=status["job_state"],
                file_states=file_states,
            )
        else:  # list_files = False
            return DatasetStatusResponse(state=status["job_state"])


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
        fts3_client (Fts3Client): Cached client for calls to FTS.

    Returns:
        CompleteResponse: Completeness of the requested job.
    """
    status = fts3_client.status(job_id=job_id)
    return CompleteResponse(complete=status["job_state"] in COMPLETE_JOB_STATES)


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
        fts3_client (Fts3Client): Cached client for calls to FTS.

    Returns:
        PercentageResponse: Percentage of individual transfers that are completed.
    """
    status = fts3_client.status(job_id=job_id, list_files=True)
    files_total = len(status["files"])
    files_complete = StateController.sum_completed_transfers(status["files"])
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


@app.get("/storage-type", summary="Get storage types for endpoints")
def get_storage_info():

    settings = get_settings()

    fts3_settings = settings.fts3

    archive_storage_type = None
    if fts3_settings.archive_endpoint:
        archive_storage_type = fts3_settings.archive_endpoint.storage_type

    storage_endpoint_type = {}
    for key, value in fts3_settings.storage_endpoints.items():
        storage_endpoint_type[key] = value.storage_type

    return {"archive": archive_storage_type, "storage": storage_endpoint_type}


@app.post(
    "/size",
    summary="Returns the size of the specified entities",
)
def size(transfer_request: TransferRequest, session_id: SessionIdDependency) -> int:

    total_size = 0
    icat_client = IcatClient(session_id)

    datafiles = icat_client.get_unique_datafiles(
        transfer_request.investigation_ids,
        transfer_request.dataset_ids,
        transfer_request.datafile_ids,
    )

    for datafile in datafiles:
        total_size += datafile.fileSize

    return total_size
