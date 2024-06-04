from datastore_api.fts3_client import Fts3Client
from datastore_api.icat_client import IcatClient
from datastore_api.models.archive import Investigation
from datastore_api.transfer_controller import DatasetArchiver


class InvestigationArchiver:
    """Handles logic for archiving at the Investigation level by iterating over its
    Datasets.
    """

    def __init__(
        self,
        session_id: str,
        icat_client: IcatClient,
        fts3_client: Fts3Client,
        investigation: Investigation,
    ) -> None:
        """Initialises the Archiver with clients and Investigation metadata.

        Args:
            session_id (str): ICAT session to use.
            icat_client (IcatClient): ICAT client to use.
            fts3_client (Fts3Client): FTS client to use.
            investigation (Investigation): Investigation metadata.
        """
        self.session_id = session_id
        self.icat_client = icat_client
        self.fts3_client = fts3_client
        self.investigation = investigation
        self.job_ids = []
        self.beans = []
        self.total_transfers = 0
        self.investigation_entity = icat_client.new_investigation(
            session_id=session_id,
            investigation=investigation,
        )

    def archive_datasets(self) -> None:
        """Iterates over the Investigation's Datasets, extending `self.job_ids` and
        `self.beans` to record the FTS jobs submitted and ICAT entities to be created
        respectively.

        If a corresponding Investigation does not yet exist, it will be recorded,
        otherwise Datasets will be.
        """
        dataset_entities = []
        for dataset in self.investigation.datasets:
            dataset_archiver = DatasetArchiver(
                session_id=self.session_id,
                icat_client=self.icat_client,
                fts3_client=self.fts3_client,
                investigation=self.investigation,
                dataset=dataset,
                investigation_entity=self.investigation_entity,
            )
            dataset_archiver.create_fts_jobs()
            dataset_entities.append(dataset_archiver.dataset_entity)
            self.job_ids.extend(dataset_archiver.job_ids)
            self.total_transfers += dataset_archiver.total_transfers

        if self.investigation_entity.id is None:
            self.investigation_entity.datasets = dataset_entities
            self.beans.append(self.investigation_entity)
        else:
            self.beans.extend(dataset_entities)
