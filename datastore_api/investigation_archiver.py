from datastore_api.fts3_client import Fts3Client
from datastore_api.icat_client import IcatClient
from datastore_api.models.icat import Dataset, Investigation, InvestigationIdentifier
from datastore_api.transfer_controller import DatasetArchiver


class InvestigationArchiver:
    """Handles logic for archiving at the Investigation level by iterating over its
    Datasets.
    """

    def __init__(
        self,
        icat_client: IcatClient,
        fts3_client: Fts3Client,
        facility_name: str,
        investigation: Investigation | InvestigationIdentifier,
        instrument_name: str = None,
        facility_cycle_name: str = None,
        datasets: list[Dataset] = None,
    ) -> None:
        """Initialises the Archiver with clients and Investigation metadata.

        Args:
            session_id (str): ICAT session to use.
            icat_client (IcatClient): ICAT client to use.
            fts3_client (Fts3Client): FTS client to use.
            facility_name (str): Name of the ICAT Facility `investigation` belongs to.
            investigation (Investigation | InvestigationIdentifier):
                Either full or identifying metadata for an Investigation.
            instrument_name (str, optional):
                Name of an ICAT Instrument, only needed if an InvestigationIdentifier
                provided. Defaults to None.
            facility_cycle_name (str, optional):
                Name of an ICAT FacilityCycle, only needed if an InvestigationIdentifier
                provided. Defaults to None.
            datasets (list[Dataset], optional):
                List of ICAT Dataset metadata, only needed if an InvestigationIdentifier
                provided. Defaults to None.
        """
        self.job_ids = []
        self.beans = []
        self.total_transfers = 0

        self.icat_client = icat_client
        self.fts3_client = fts3_client
        self.facility_name = facility_name

        if isinstance(investigation, Investigation):
            instrument_name = investigation.instrument.name
            facility_cycle_name = investigation.facilityCycle.name
            datasets = investigation.datasets

        self.investigation_path = IcatClient._build_path(
            instrument_name=instrument_name,
            cycle_name=facility_cycle_name,
            investigation_name=investigation.name,
            visit_id=investigation.visitId,
            dataset_type_name="{dataset_type_name}",
            dataset_name="{dataset_name}",
            datafile_name="{datafile_name}",
        )
        self.datasets = datasets
        self.investigation_entity = icat_client.new_investigation(
            facility_name=facility_name,
            investigation=investigation,
        )
        if self.investigation_entity.id is None:
            self.investigation_entity.id = icat_client.client.create(
                self.investigation_entity,
            )

    def archive_datasets(self) -> None:
        """Iterates over the Investigation's Datasets, extending `self.job_ids` and
        `self.beans` to record the FTS jobs submitted and ICAT entities to be created
        respectively.

        If a corresponding Investigation does not yet exist, it will be recorded,
        otherwise Datasets will be.
        """
        for dataset in self.datasets:
            dataset_archiver = DatasetArchiver(
                icat_client=self.icat_client,
                fts3_client=self.fts3_client,
                facility_name=self.facility_name,
                investigation_path=self.investigation_path,
                dataset=dataset,
                investigation_entity=self.investigation_entity,
            )
            dataset_archiver.create_fts_jobs()
            self.beans.append(dataset_archiver.dataset_entity)
            self.job_ids.extend(dataset_archiver.job_ids)
            self.total_transfers += dataset_archiver.total_transfers
