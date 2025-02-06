from datetime import datetime

from fastapi import HTTPException
from icat.entity import Entity

from datastore_api.clients.fts3_client import get_fts3_client
from datastore_api.clients.icat_client import get_icat_cache, IcatClient
from datastore_api.clients.s3_client import get_s3_client
from datastore_api.clients.x_root_d_client import get_x_root_d_client
from datastore_api.config import S3Storage, StorageType, TapeStorage
from datastore_api.controllers.bucket_controller import BucketController
from datastore_api.models.dataset import DatasetStatusListFilesResponse
from datastore_api.models.icat import Dataset
from datastore_api.models.job import COMPLETE_JOB_STATES, JobState, TransferState
from datastore_api.models.transfer import (
    BucketAcl,
    TransferResponse,
    TransferS3Response,
)


class TransferController:
    """ABC for controlling and batching requests to the Fts3Client."""

    def __init__(
        self,
        datafile_entities: list[Entity],
        source_key: str = None,
        destination_key: str = None,
        bucket_acl: BucketAcl = None,
    ) -> None:
        """Initialises the TransferController with the Datafiles to move, and the
        details of the source and destination.

        Args:
            datafile_entities (list[Entity]): List of ICAT Datafile Entities to move.
            source_key (str, optional):
                Key identifying the source storage.
                If None, the archive storage will be used. Defaults to None.
            destination_key (str, optional):
                Key identifying the destination storage.
                If None, the archive storage will be used. Defaults to None.
            bucket_acl (BucketAcl, optional):
                Canned Access Control List for the destination S3 bucket,
                only needed for transfers to S3. Defaults to None.
        """
        self.fts3_client = get_fts3_client()
        self.datafile_entities = datafile_entities
        self.transfers = []
        self.job_ids = []
        self.bring_online = -1
        self.archive_timeout = -1
        self.strict_copy = False
        self.total_transfers = 0
        self.total_size = 0
        self.source_key = source_key
        self.source_storage = self.fts3_client.get_storage(key=source_key)
        self.source_prefix = ""
        self.destination_storage = self.fts3_client.get_storage(key=destination_key)
        self.destination_prefix = ""
        self.bucket_controller = None
        self.size = 0

        if isinstance(self.source_storage, S3Storage):
            self.source_prefix = f"{self.source_storage.cache_bucket}/"
        elif isinstance(self.source_storage, TapeStorage):
            self.bring_online = self.source_storage.bring_online

        if isinstance(self.destination_storage, S3Storage):
            self.strict_copy = True
            self.bucket_controller = BucketController(storage_key=destination_key)
            self.bucket_controller.create(bucket_acl=bucket_acl)
            if bucket_acl == BucketAcl.PUBLIC_READ:
                self.destination_prefix = f"{self.destination_storage.cache_bucket}/"
            else:
                self.destination_prefix = f"{self.bucket_controller.bucket.name}/"
        elif isinstance(self.destination_storage, TapeStorage):
            self.archive_timeout = self.destination_storage.archive_timeout

    def create_fts_jobs(self) -> TransferS3Response | TransferResponse:
        """Iterates over `self.paths`, creating and submitting transfers to FTS as
        needed.

        Returns:
            TransferS3Response | TransferResponse: Scheduled FTS job ids.
        """
        for datafile_entity in self.datafile_entities:
            transfer = self._transfer(datafile_entity)
            self.transfers.append(transfer)
            if datafile_entity.fileSize is not None:
                self.size += datafile_entity.fileSize

        self._submit_all()

        if self.bucket_controller is not None:
            job_states = {j: JobState.submitted for j in self.job_ids}
            self.bucket_controller.set_job_ids(job_states=job_states)
            return TransferS3Response(
                job_ids=self.job_ids,
                bucket_name=self.bucket_controller.bucket.name,
            )

        return TransferResponse(job_ids=self.job_ids, size=self.size)

    def _check_source(self, datafile_entity: Entity) -> None:
        """Check source storage for presence of the target file, and extract its size.

        Args:
            datafile_entity (Entity):
                ICAT Datafile Entity. Datafile.size modified in place.
        """
        if self.fts3_client.fts3_settings.check_source:
            if self.source_storage.storage_type == StorageType.S3:
                s3_client = get_s3_client(key=self.source_key)
                stat_info = s3_client.stat(datafile_entity.location)
                datafile_entity.fileSize = stat_info["ContentLength"]
                datafile_entity.datafileModTime = stat_info["LastModified"]
            else:
                x_root_d_client = get_x_root_d_client(url=self.source_storage.url)
                stat_info = x_root_d_client.stat(datafile_entity.location)
                datafile_entity.fileSize = stat_info.size
                datafile_entity.datafileModTime = datetime.fromtimestamp(
                    stat_info.modtime,
                )

    def _validate_file_size(self, file_size: int) -> None:
        """
        Args:
            file_size (int): Size to check.

        Raises:
            HTTPException: If file_size exceeds the configured limit.
        """
        if file_size is not None:
            self.total_size += file_size
            size_limit = self.fts3_client.fts3_settings.file_size_limit
            if size_limit is not None and file_size > size_limit:
                detail = (
                    f"Cannot accept file of size {file_size} "
                    f"due to limit of {size_limit}"
                )
                raise HTTPException(status_code=400, detail=detail)

    def _validate_total_size(self) -> None:
        """
        Raises:
            HTTPException: If self.total_size exceeds the configured limit.
        """
        size_limit = self.fts3_client.fts3_settings.total_file_size_limit
        if size_limit is not None and self.total_size > size_limit:
            detail = (
                f"Cannot accept transfer request of total size {self.total_size} "
                f"due to limit of {size_limit}"
            )
            raise HTTPException(status_code=400, detail=detail)

    def _transfer(self, datafile_entity: Entity) -> dict[str, list]:
        """Returns a transfer dict moving `path` from one of the caches to tape.

        Args:
            datafile_entity (Entity): ICAT Datafile Entity to transfer.

        Returns:
            dict[str, list]: Transfer dict for moving `path` to tape.
        """
        self._check_source(datafile_entity)
        self._validate_file_size(datafile_entity.fileSize)
        return self.fts3_client.transfer(
            datafile_entity=datafile_entity,
            source_storage=self.source_storage,
            source_prefix=self.source_prefix,
            destination_storage=self.destination_storage,
            destination_prefix=self.destination_prefix,
        )

    def _submit_all(self, maximum_transfers: int = 1000) -> None:
        """Submits all pending `self.transfers`.

        Args:
            maximum_transfers (int, optional):
                Will submit jobs of up to the many transfers. Allows batching of
                transfers whilst limiting JSON length of the request. Defaults to 1.
        """
        self._validate_total_size()
        for i in range(0, len(self.transfers), maximum_transfers):
            lower = maximum_transfers * i
            upper = maximum_transfers * (i + 1)
            transfer_block = self.transfers[lower:upper]
            job_id = self.fts3_client.submit(
                transfers=transfer_block,
                bring_online=self.bring_online,
                archive_timeout=self.archive_timeout,
                strict_copy=self.strict_copy,
            )
            self.job_ids.append(job_id)
            self.total_transfers += len(transfer_block)


class DatasetArchiver(TransferController):
    """Controller for archiving paths to tape, generated from a Dataset entity."""

    def __init__(
        self,
        icat_client: IcatClient,
        dataset: Dataset,
        investigation_entity: Entity,
        source_key: str,
    ) -> None:
        """Initialises the controller with the clients and metadata to use.

        Args:
            icat_client (IcatClient): ICAT client to use.
            dataset (Dataset): Dataset metadata.
            investigation_entity (Entity): ICAT Investigation entity.
            source_key (str): FTS storage endpoint to use as source.
        """
        dataset_entity = icat_client.new_dataset(
            dataset=dataset,
            investigation_entity=investigation_entity,
        )
        super().__init__(
            datafile_entities=dataset_entity.datafiles,
            source_key=source_key,
            destination_key=None,
        )
        self.source_key = source_key
        self.icat_client = icat_client
        self.dataset_entity = dataset_entity

    def create_fts_jobs(self) -> None:
        """Iterates over `self.paths`, creating and submitting transfers to FTS as
        needed.

        Also sets the FTS job ids on the relevant DatasetParameter.
        """
        super().create_fts_jobs()
        type_job_ids = self.icat_client.settings.parameter_type_job_ids
        joined_job_ids = ",".join(self.job_ids)
        for parameter in self.dataset_entity.parameters:
            if parameter.type.name == type_job_ids:
                parameter.stringValue = joined_job_ids
                return


class DatasetReArchiver(TransferController):
    """Controller for re-archiving paths to tape, generated from a Dataset entity."""

    def __init__(
        self,
        icat_client: IcatClient,
        dataset_id: int,
        status: DatasetStatusListFilesResponse,
        source_key: str,
    ) -> None:
        """Initialises the DatasetReArchiver with the Dataset to retry, and the details
        of the destination.

        Args:
            icat_client (IcatClient): IcatClient to get and set the required metadata.
            dataset_id (int): ICAT Entity id of the Dataset to retry.
            status (DatasetStatusListFilesResponse): Status of the Dataset to retry.
            source_key (str): Key identifying the source storage.
        """
        DatasetReArchiver._validate_status(status)
        self.icat_client = icat_client
        self.dataset_entity = icat_client.get_single_entity(
            entity="Dataset",
            equals={"id": dataset_id},
            includes=[
                "investigation",
                "type",
                "parameters.type",
                "datafiles.parameters.type",
            ],
        )
        datafile_entities = []
        for datafile in self.dataset_entity.datafiles:
            state = status.file_states[datafile.location]
            if state != TransferState.finished:
                datafile_entities.append(datafile)

        super().__init__(
            datafile_entities=datafile_entities,
            source_key=source_key,
            destination_key=None,
        )

    @staticmethod
    def _validate_status(status: DatasetStatusListFilesResponse) -> None:
        """
        Args:
            status (DatasetStatusListFilesResponse): Status of the Dataset to retry.

        Raises:
            HTTPException: If status is in progress or finished with no failures.
        """
        if status.state == JobState.finished:
            detail = "Archival completed successfully, nothing to retry"
            raise HTTPException(400, detail)
        elif status.state not in COMPLETE_JOB_STATES:
            raise HTTPException(400, "Archival not yet complete, cannot retry")

    def _reset_state_parameter(self, parameters: list[Entity]) -> None:
        """Resets the archival state parameter to SUBMITTED.

        Args:
            parameters (list[Entity]): ICAT Parameter Entities for the Entity to retry.
        """
        type_job_state = self.icat_client.settings.parameter_type_job_state
        for parameter in parameters:
            if parameter.type.name == type_job_state:
                parameter.get()
                parameter.stringValue = "SUBMITTED"
                parameter.update()

    def create_fts_jobs(self) -> None:
        """Iterates over `self.paths`, creating and submitting transfers to FTS as
        needed.

        Also sets the FTS job ids on the relevant DatasetParameter.
        """
        super().create_fts_jobs()
        icat_cache = get_icat_cache()
        self._reset_state_parameter(self.dataset_entity.parameters)

        parameter = self.icat_client.client.new(
            "DatasetParameter",
            type=icat_cache.parameter_type_job_ids,
            stringValue=",".join(self.job_ids),
            dataset=self.dataset_entity,
        )
        self.icat_client.client.create(parameter)

    def _transfer(self, datafile_entity: Entity) -> dict[str, list]:
        """Returns a transfer dict moving `path` from one of the caches to tape.

        Args:
            path (str): Path of the file to be moved.

        Returns:
            dict[str, list]: Transfer dict for moving `path` to tape.
        """
        self._reset_state_parameter(datafile_entity.parameters)
        return super()._transfer(datafile_entity=datafile_entity)
