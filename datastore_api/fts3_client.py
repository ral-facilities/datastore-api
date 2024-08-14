from functools import lru_cache
import logging

import fts3.rest.client.easy as fts3
from icat.entity import Entity

from datastore_api.config import get_settings, VerifyChecksum


LOGGER = logging.getLogger(__name__)


class Fts3Client:
    """Wrapper for FTS3 functionality."""

    def __init__(self) -> None:
        """Initialise the client."""
        fts_settings = get_settings().fts3
        self.context = fts3.Context(
            endpoint=fts_settings.endpoint,
            ucert=fts_settings.x509_user_cert,
            ukey=fts_settings.x509_user_key,
        )
        self.instrument_data_cache = fts_settings.instrument_data_cache
        self.restored_data_cache = fts_settings.restored_data_cache
        self.tape_archive = fts_settings.tape_archive
        self.retry = fts_settings.retry
        self.verify_checksum = fts_settings.verify_checksum
        self.supported_checksums = fts_settings.supported_checksums
        self.bring_online = fts_settings.bring_online
        self.archive_timeout = fts_settings.archive_timeout

    def archive(self, datafile_entity: Entity) -> dict[str, list]:
        """Returns a transfer dict moving `path` from one of the caches to tape.

        Args:
            datafile_entity (Entity): Datafile to be moved.

        Returns:
            dict[str, list]: Transfer dict for moving `path` to tape.
        """
        source = f"{self.instrument_data_cache}{datafile_entity.location}"
        alternate_source = f"{self.restored_data_cache}{datafile_entity.location}"
        destination = f"{self.tape_archive}{datafile_entity.location}"
        checksum = self._validate_checksum(datafile_entity.checksum)
        transfer = fts3.new_transfer(
            source=source,
            destination=destination,
            checksum=checksum,
        )
        transfer["sources"].append(alternate_source)
        return transfer

    def restore(self, datafile_entity: Entity) -> dict[str, list]:
        """Returns a transfer dict moving `path` from tape to the RDC.

        Args:
            datafile_entity (Entity): Datafile to be moved.

        Returns:
            dict[str, list]: Transfer dict for moving `path` to the RDC.
        """
        source = f"{self.tape_archive}{datafile_entity.location}"
        destination = f"{self.restored_data_cache}{datafile_entity.location}"
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

    def submit(self, transfers: list[dict[str, list]], stage: bool = False) -> str:
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
            bring_online=self.bring_online if stage else -1,
            archive_timeout=self.archive_timeout if not stage else -1,
        )
        return fts3.submit(context=self.context, job=job)

    def status(self, job_id: str, list_files: bool = False) -> dict:
        """Get full status dict (including state) for an FTS job.

        Args:
            job_id (str): UUID4 for an FTS job.
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
