import fts3.rest.client.easy as fts3

from datastore_api.config import Fts3Settings


class Fts3Client:
    """Wrapper for FTS3 functionality."""

    def __init__(self, fts_settings: Fts3Settings) -> None:
        """Initialise the client with the provided `fts_settings`.

        Args:
            fts_settings (Fts3Settings): Settings for FTS3 operations.
        """
        self.context = fts3.Context(
            endpoint=fts_settings.endpoint,
            ucert=fts_settings.x509_user_cert,
            ukey=fts_settings.x509_user_key,
        )
        self.instrument_data_cache = fts_settings.instrument_data_cache
        self.user_data_cache = fts_settings.user_data_cache
        self.tape_archive = fts_settings.tape_archive
        self.bring_online = fts_settings.bring_online
        self.copy_pin_lifetime = fts_settings.copy_pin_lifetime

    def archive(self, path: str) -> dict[str, list]:
        """Returns a transfer dict moving `path` from one of the caches to tape.

        Args:
            path (str): Path of the file to be moved.

        Returns:
            dict[str, list]: Transfer dict for moving `path` to tape.
        """
        source = f"{self.instrument_data_cache}/{path}"
        alternate_source = f"{self.user_data_cache}/{path}"
        destination = f"{self.tape_archive}/{path}"
        transfer = fts3.new_transfer(source=source, destination=destination)
        transfer["sources"].append(alternate_source)
        return transfer

    def restore(self, path: str) -> dict[str, list]:
        """Returns a transfer dict moving `path` from tape to the UDC.

        Args:
            path (str): Path of the file to be moved.

        Returns:
            dict[str, list]: Transfer dict for moving `path` to the UDC.
        """
        source = f"{self.tape_archive}/{path}"
        destination = f"{self.user_data_cache}/{path}"
        return fts3.new_transfer(source=source, destination=destination)

    def submit(self, transfers: list[dict[str, list]], stage: bool = False) -> str:
        """Submit a single FTS job for the `transfers`.

        Args:
            transfers (list[dict[str, list]]):
                FTS transfer dicts to be submitted as one job.
            stage (bool): Whether the job requires staging from tape before transfer.

        Returns:
            str: FTS job id (UUID4).
        """
        job = fts3.new_job(
            transfers=transfers,
            bring_online=self.bring_online if stage else None,
            copy_pin_lifetime=self.copy_pin_lifetime if stage else None,
            verify_checksum="none",
        )
        return fts3.submit(context=self.context, job=job)

    def status(self, job_id: str) -> dict:
        """Get full status dict (including state) for an FTS job.

        Args:
            job_id (str): UUID4 for an FTS job.

        Returns:
            dict: FTS status dict for `job_id`.
        """
        return fts3.get_job_status(context=self.context, job_id=job_id)

    def cancel(self, job_id: str) -> str:
        """Cancel an FTS job.

        Args:
            job_id (str): UUID4 for an FTS job.

        Returns:
            str: The terminal state of the FTS job.
        """
        return fts3.cancel(context=self.context, job_id=job_id)
