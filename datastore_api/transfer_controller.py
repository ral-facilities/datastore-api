from abc import ABC, abstractmethod

from icat.entity import Entity

from datastore_api.fts3_client import Fts3Client
from datastore_api.icat_client import IcatClient
from datastore_api.models.icat import Dataset


class TransferController(ABC):
    """ABC for controlling and batching requests to the Fts3Client."""

    def __init__(self, fts3_client: Fts3Client, strict_copy: bool = False) -> None:
        """Initialises the controller with the Fts3Client to use.

        Args:
            fts3_client (Fts3Client): The Fts3Client to use for transfers and jobs.
            strict_copy (bool): Should be True for transfers to S3 endpoints.
        """
        self.fts3_client = fts3_client
        self.datafile_entities = []
        self.transfers = []
        self.job_ids = []
        self.stage = False
        self.total_transfers = 0
        self.strict_copy = strict_copy

    def create_fts_jobs(self) -> None:
        """Iterates over `self.paths`, creating and submitting transfers to FTS as
        needed.
        """
        for datafile_entity in self.datafile_entities:
            transfer = self._transfer(datafile_entity)
            self.transfers.append(transfer)
            self._submit(minimum_transfers=1000)

        self._submit()

    @abstractmethod
    def _transfer(self, datafile_entity: Entity) -> dict[str, list]: ...

    def _submit(self, minimum_transfers: int = 1) -> None:
        """Submits any pending `self.transfers`.

        Args:
            minimum_transfers (int, optional):
                Will only submit `self.transfers` If there are at least this many
                pending. Allows batching of transfers whilst limiting JSON length of
                the request. Defaults to 1.
        """
        if len(self.transfers) >= minimum_transfers:
            job_id = self.fts3_client.submit(
                self.transfers,
                self.stage,
                self.strict_copy,
            )
            self.job_ids.append(job_id)
            self.total_transfers += len(self.transfers)
            self.transfers = []


class RestoreController(TransferController):
    """Controller for restoring paths to disk or download cache,
    regardless of origin.
    """

    def __init__(
        self,
        fts3_client: Fts3Client,
        destination_cache: str,
        datafile_entities: list[Entity],
        strict_copy: bool = False,
    ) -> None:
        """Initialises the controller with the Fts3Client and paths to use.

        Args:
            fts3_client (Fts3Client): The Fts3Client to use for transfers and jobs.
            destination_cache (str): Cache to restore file to
            datafile_entities (list[Entity]): Datafiles to restore.
            strict_copy (bool): Should be True for transfers to S3 endpoints.
        """
        super().__init__(fts3_client, strict_copy)
        self.datafile_entities = datafile_entities
        self.stage = True
        self.destination_cache = destination_cache

    def _transfer(self, datafile_entity: Entity) -> dict[str, list]:
        """Returns a transfer dict moving `datafile_entity` from tape to the RDC.

        Args:
            datafile_entity (Entity): Path of the file to be moved.

        Returns:
            dict[str, list]: Transfer dict for moving `path` to the RDC.
        """
        return self.fts3_client.restore(datafile_entity, self.destination_cache)


class DatasetArchiver(TransferController):
    """Controller for archiving paths to tape, generated from a Dataset entity."""

    def __init__(
        self,
        icat_client: IcatClient,
        fts3_client: Fts3Client,
        facility_name: str,
        investigation_path: str,
        dataset: Dataset,
        investigation_entity: Entity,
    ) -> None:
        """Initialises the controller with the clients and metadata to use.

        Args:
            session_id (str): ICAT session_id.
            icat_client (IcatClient): ICAT client to use.
            fts3_client (Fts3Client): The Fts3Client to use for transfers and jobs.
            investigation (Investigation): Investigation metadata.
            dataset (Dataset): Dataset metadata.
            investigation_entity (Entity): ICAT Investigation entity.
        """
        super().__init__(fts3_client)
        dataset_entity = icat_client.new_dataset(
            facility_name=facility_name,
            investigation_path=investigation_path,
            dataset=dataset,
            investigation_entity=investigation_entity,
        )
        self.icat_client = icat_client
        self.dataset_entity = dataset_entity
        self.datafile_entities = dataset_entity.datafiles

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

    def _transfer(self, datafile_entity: Entity) -> dict[str, list]:
        """Returns a transfer dict moving `path` from one of the caches to tape.

        Args:
            path (str): Path of the file to be moved.

        Returns:
            dict[str, list]: Transfer dict for moving `path` to tape.
        """
        return self.fts3_client.archive(datafile_entity)
