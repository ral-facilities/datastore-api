from functools import lru_cache

import fts3.rest.client.easy as fts3

from datastore_api.config import get_settings


class Fts3Client:
    """Wrapper for FTS3 functionality."""

    def __init__(self) -> None:
        """Initialise the client with the provided `fts_settings`.

        Args:
            fts_settings (Fts3Settings): Settings for FTS3 operations.
        """
        settings = get_settings()
        self.context = fts3.Context(
            endpoint=settings.fts3.endpoint,
            ucert=settings.fts3.x509_user_cert,
            ukey=settings.fts3.x509_user_key,
        )
        self.instrument_data_cache = settings.fts3.instrument_data_cache
        self.user_data_cache = settings.fts3.user_data_cache
        self.download_cache = settings.s3.endpoint
        self.tape_archive = settings.fts3.tape_archive
        self.retry = settings.fts3.retry
        self.verify_checksum = settings.fts3.verify_checksum
        self.bring_online = settings.fts3.bring_online
        self.archive_timeout = settings.fts3.archive_timeout

    def archive(self, path: str) -> dict[str, list]:
        """Returns a transfer dict moving `path` from one of the caches to tape.

        Args:
            path (str): Path of the file to be moved.

        Returns:
            dict[str, list]: Transfer dict for moving `path` to tape.
        """
        source = f"{self.instrument_data_cache}{path}"
        alternate_source = f"{self.user_data_cache}{path}"
        destination = f"{self.tape_archive}{path}"
        transfer = fts3.new_transfer(source=source, destination=destination)
        transfer["sources"].append(alternate_source)
        return transfer

    def restore(self, path: str, destination_cache: str) -> dict[str, list]:
        """Returns a transfer dict moving `path` from tape to the UDC.

        Args:
            path (str): Path of the file to be moved.
            destination_cache (str): URL of the destination cache.

        Returns:
            dict[str, list]: Transfer dict for moving `path` to the UDC.
        """
        source = (
            f"{self.tape_archive}{path}"
            f"{'?copy_mode=push' if self.download_cache in destination_cache else ''}"
        )
        destination = f"{destination_cache}{path}"
        return fts3.new_transfer(source=source, destination=destination)

    def submit(
        self,
        transfers: list[dict[str, list]],
        stage: bool = False,
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
            bring_online=self.bring_online if stage else -1,
            archive_timeout=self.archive_timeout if not stage else -1,
            strict_copy=strict_copy,
        )
        return fts3.submit(context=self.context, job=job)

    def status(
        self,
        job_id: str | list[str],
        list_files: bool = False,
    ) -> list[dict]:
        """Get full status dicts (including state) for one or more FTS jobs.

        Args:
            job_id (str or list): UUID4 for an FTS job.
            list_files (bool, optional):
                If True, will return the list of individual file statuses.
                Defaults to False.

        Returns:
            list[dict]: FTS status dicts for `job_id`.
        """
        if type(job_id) is str:
            job_id = list(job_id)

        return fts3.get_jobs_statuses(
            context=self.context,
            job_ids=job_id,
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
