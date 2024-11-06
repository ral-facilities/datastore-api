from functools import lru_cache
import logging

import fts3.rest.client.easy as fts3
from icat.entity import Entity

from datastore_api.config import get_settings, Storage, StorageType, VerifyChecksum


LOGGER = logging.getLogger(__name__)


class Fts3Client:
    """Wrapper for FTS3 functionality."""

    def __init__(self) -> None:
        """Initialise the client."""
        settings = get_settings()
        self.context = fts3.Context(
            endpoint=settings.fts3.endpoint,
            verify=settings.fts3.verify,
            ucert=settings.fts3.x509_user_cert,
            ukey=settings.fts3.x509_user_key,
        )
        self.fts3_settings = settings.fts3
        self.retry = settings.fts3.retry
        self.verify_checksum = settings.fts3.verify_checksum
        self.supported_checksums = settings.fts3.supported_checksums

    @staticmethod
    def _validate_statuses(statuses: list[dict] | dict) -> list[dict]:
        # FTS will actually return a single dict if a length 1 list is provided
        if isinstance(statuses, dict):
            return [statuses]
        else:
            return statuses

    @staticmethod
    def _format_location(
        location: str,
        source_storage: Storage,
        source_prefix: str,
        destination_storage: Storage,
        destination_prefix: str,
    ) -> tuple[str, str]:
        """Formats source and destination surls for FTS.

        Args:
            location (str): File location on storage.
            source_storage (Storage): Representation of the source Storage.
            source_prefix (str): Prefix to before the location for the source.
            destination_storage (Storage): Representation of the destination Storage.
            destination_prefix (str): Prefix to before the location for the destination.

        Returns:
            tuple[str, str]: source, destination
        """
        if destination_storage.storage_type == StorageType.S3:
            query = "?copy_mode=push"
        else:
            query = ""

        return (
            f"{source_storage.formatted_url}{source_prefix}{location}{query}",
            f"{destination_storage.formatted_url}{destination_prefix}{location}",
        )

    def get_storage(self, key: str) -> Storage:
        """Get the representation of configured Storage.

        Args:
            key (str):
                Key to identify the Storage. If None, the archive storage is returned.

        Returns:
            Storage: Representation of the requested Storage.
        """
        if key is None:
            return self.fts3_settings.archive_endpoint

        return self.fts3_settings.storage_endpoints[key]

    def transfer(
        self,
        datafile_entity: Entity,
        source_storage: Storage,
        source_prefix: str,
        destination_storage: Storage,
        destination_prefix: str,
    ) -> dict[str, list]:
        """Returns a transfer dict moving `path` from tape to another storage endpoint.

        Args:
            datafile_entity (Entity): Datafile to be moved.
            source_storage (Storage): Representation of the source Storage.
            source_prefix (str): Prefix to before the location for the source.
            destination_storage (Storage): Representation of the destination Storage.
            destination_prefix (str): Prefix to before the location for the destination.

        Returns:
            dict[str, list]: Transfer dict for moving `path` to the RDC.
        """
        source, destination = Fts3Client._format_location(
            location=datafile_entity.location,
            source_storage=source_storage,
            source_prefix=source_prefix,
            destination_storage=destination_storage,
            destination_prefix=destination_prefix,
        )
        checksum = self._validate_checksum(datafile_entity.checksum)
        return fts3.new_transfer(
            source=source,
            destination=destination,
            checksum=checksum,
        )

    def _validate_checksum(self, checksum: str | None) -> str | None:
        """Validates a Datafile checksum against `self.supported_checksums` and
        `self.verify_checksum`.

        Args:
            checksum (str | None): Checksum for a Datafile in the form mechanism:value.

        Returns:
            str | None: If valid, returns `checksum`, else None.
        """
        checksum_mechanism = None
        checksum_value = None
        if checksum:
            checksum_parts = checksum.split(":")
            if checksum_parts[0] in self.supported_checksums:
                checksum_mechanism = checksum_parts[0]
            else:
                msg = "%s not in list of supported checksum mechanisms: %s"
                LOGGER.warning(msg, checksum_parts[0], self.supported_checksums)
                return None

            if len(checksum_parts) > 1:
                checksum_value = ":".join(checksum_parts[1:])

        if self.verify_checksum in {VerifyChecksum.SOURCE, VerifyChecksum.DESTINATION}:
            if checksum_mechanism is None or checksum_value is None:
                msg = (
                    "Both mechanism and value must be specified for checksum "
                    "verification at %s"
                )
                LOGGER.warning(msg, self.verify_checksum)
                return None
            else:
                return checksum

        elif self.verify_checksum == VerifyChecksum.BOTH:
            if checksum_mechanism is None:
                msg = "Mechanism must be specified for checksum verification at %s"
                LOGGER.warning(msg, self.verify_checksum)
                return None
            else:
                return checksum

        else:
            return None

    def submit(
        self,
        transfers: list[dict[str, list]],
        bring_online: int = -1,
        archive_timeout: int = -1,
        strict_copy: bool = False,
    ) -> str:
        """Submit a single FTS job for the `transfers`.

        Args:
            transfers (list[dict[str, list]]):
                FTS transfer dicts to be submitted as one job.
            stage (bool, optional):
                Whether the job requires staging from tape before transfer.
                Defaults to False.

        Returns:
            str: FTS job id (UUID4).
        """
        job = fts3.new_job(
            transfers=transfers,
            retry=self.retry,
            verify_checksum=self.verify_checksum.value,
            bring_online=bring_online,
            archive_timeout=archive_timeout,
            strict_copy=strict_copy,
        )
        LOGGER.debug("Submitting job to FTS: %s", job)
        return fts3.submit(context=self.context, job=job)

    def status(
        self,
        job_id: str,
        list_files: bool = False,
    ) -> list[dict]:
        """Get full status dict (including state) for an FTS job.

        Args:
            job_id (list[str]): UUID4 for an FTS job.
            list_files (bool, optional):
                If True, will return the list of individual file statuses.
                Defaults to False.

        Returns:
            dict: FTS status dict for `job_id`.
        """
        return fts3.get_job_status(
            context=self.context,
            job_id=job_id,
            list_files=list_files,
        )

    def statuses(
        self,
        job_ids: list[str],
        list_files: bool = False,
    ) -> list[dict]:
        """Get full status dicts (including state) for FTS jobs.

        Args:
            job_ids (list[str]): UUID4s for FTS jobs.
            list_files (bool, optional):
                If True, will return the list of individual file statuses.
                Defaults to False.

        Returns:
            list[dict]: FTS status dicts for `job_id`.
        """
        statuses = fts3.get_jobs_statuses(
            context=self.context,
            job_ids=job_ids,
            list_files=list_files,
        )
        return Fts3Client._validate_statuses(statuses=statuses)

    def cancel(self, job_id: str) -> str:
        """Cancel an FTS job.

        Args:
            job_id (str): UUID4 for an FTS job.

        Returns:
            str: The terminal state of the FTS job.
        """
        return fts3.cancel(context=self.context, job_id=job_id)


@lru_cache
def get_fts3_client() -> Fts3Client:
    """Initialise and cache the client for making calls to FTS.

    Returns:
        FtsClient: Wrapper for calls to FTS.
    """
    return Fts3Client()
