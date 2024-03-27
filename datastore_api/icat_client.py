from functools import wraps
from typing import Any, Callable

from fastapi import HTTPException
from icat import Client, ICATSessionError
from icat.entity import Entity, EntityList
from icat.query import Query

from datastore_api.config import IcatSettings, IcatUser
from datastore_api.models.archive import Datafile, Dataset, Investigation
from datastore_api.models.login import LoginRequest


def handle_icat_session(func: Callable):
    """Sets and un-sets the Client's sessionId before and after the execution of `func`,
    and re-raises `ICATSessionError`s.

    Args:
        func (Callable): function from `IcatClient`.

    Raises:
        HTTPException: If the session_id is not valid.

    Returns:
        _Wrapped: Wrapped `func`.
    """

    @wraps(func)
    def _handle_icat_session(
        self: "IcatClient",
        session_id: str = None,
        *args,
        **kwargs,
    ) -> Any:
        try:
            self.client.sessionId = session_id
            return func(self, *args, **kwargs)
        except ICATSessionError as e:
            raise HTTPException(status_code=401, detail=e.message) from e
        finally:
            self.client.sessionId = None

    return _handle_icat_session


class IcatClient:
    """Wrapper for ICAT functionality."""

    def __init__(self, icat_settings: IcatSettings):
        """Initialise the Client with the provided `icat_settings`.

        Args:
            settings (IcatSettings): Settings for the ICAT client and admin users.
        """
        self.icat_settings = icat_settings
        self.client = Client(icat_settings.url, checkCert=icat_settings.check_cert)

    @staticmethod
    def build_path(
        instrument_name: str,
        cycle_name: str,
        investigation_name: str,
        visit_id: str,
    ) -> str:
        """Creates a deterministic path from ICAT Investigation metadata.

        Args:
            instrument_name (str): ICAT Instrument name.
            cycle_name (str): ICAT FacilityCycle name.
            investigation_name (str): ICAT Investigation name.
            visit_id (str): ICAT Investigation visitId.

        Returns:
            str: Path for FTS.
        """
        return f"/{instrument_name}/{cycle_name}/{investigation_name}-{visit_id}"

    @staticmethod
    def validate_entities(entities: EntityList, expected_ids: list[int]) -> None:
        """Check that the expected number of entities are returned from ICAT.

        Args:
            entities (EntityList): Entities returned from ICAT.
            expected_ids (list[int]): ICAT IDs that were requested.

        Raises:
            HTTPException: If the number of returned and expected entities differs.
        """
        if len(entities) != len(expected_ids):
            raise HTTPException(status_code=403, detail="insufficient permissions")

    @handle_icat_session
    def login(self, login_request: LoginRequest) -> str:
        """Uses the provided credentials to generate and ICAT sessionId.

        Args:
            login_request (LoginRequest):
                ICAT user credentials and authentication method.

        Returns:
            str: ICAT sessionId
        """
        return self.client.login(login_request.auth, login_request.credentials.dict())

    @handle_icat_session
    def authorise_admin(self) -> None:
        """Checks the sessionId belonging to the current user is for an admin.

        Raises:
            HTTPException: If the user is not one of the configured admin users.
        """
        user = self.client.getUserName()
        auth, username = user.split("/")
        if IcatUser(auth=auth, username=username) not in self.icat_settings.admin_users:
            raise HTTPException(status_code=403, detail="insufficient permissions")

    @handle_icat_session
    def create_entities(
        self,
        investigations: list[Investigation],
    ) -> list[str]:
        """Creates Investigations and child Datasets/Datafiles in ICAT and returns their
        paths for use with FTS.

        Args:
            investigations (list[Investigation]):
                Metadata for the Investigations to be created.

        Returns:
            list[str]: Paths to the Investigations in FTS.
        """
        beans = []
        paths = []
        for investigation in investigations:
            investigation_entity = self.new_investigation(investigation)
            beans.append(investigation_entity)

            path = IcatClient.build_path(
                instrument_name=investigation.instrument.name,
                cycle_name=investigation.facilityCycle.name,
                investigation_name=investigation.name,
                visit_id=investigation.visitId,
            )
            paths.append(path)

        self.client.createMany(beans=beans)
        return paths

    def new_investigation(self, investigation: Investigation) -> Entity:
        """Create a new ICAT Investigation Entity and child Dataset/Datafiles.

        Args:
            investigation (Investigation): Metadata for the Investigation to be created.

        Returns:
            Entity: The new ICAT Investigation Entity.
        """
        investigation_dict = investigation.excluded_dict()

        # Get existing high level metadata
        facility = self.get_single_entity(
            entity="Facility",
            name=investigation.facility.name,
        )
        investigation_type = self.get_single_entity(
            entity="InvestigationType",
            name=investigation.investigationType.name,
            facility_name=investigation.facility.name,
        )
        facility_cycle = self.get_single_entity(
            entity="FacilityCycle",
            name=investigation.facilityCycle.name,
            facility_name=investigation.facility.name,
        )
        instrument = self.get_single_entity(
            entity="Instrument",
            name=investigation.instrument.name,
            facility_name=investigation.facility.name,
        )

        # Create many to many relationships
        investigation_facility_cycle = self.client.new(
            obj="InvestigationFacilityCycle",
            facilityCycle=facility_cycle,
        )
        investigation_instrument = self.client.new(
            obj="InvestigationInstrument",
            instrument=instrument,
        )

        dataset_entities = []
        for dataset in investigation.datasets:
            dataset_entity = self.new_dataset(dataset, investigation.facility.name)
            dataset_entities.append(dataset_entity)

        return self.client.new(
            "Investigation",
            facility=facility,
            type=investigation_type,
            investigationFacilityCycles=[investigation_facility_cycle],
            investigationInstruments=[investigation_instrument],
            datasets=dataset_entities,
            **investigation_dict,
        )

    def new_dataset(self, dataset: Dataset, facility_name: str) -> Entity:
        """Create a new ICAT Dataset Entity and child Datafiles.

        Args:
            dataset (Dataset): Metadata for the Dataset to be created.
            facility_name (str): Name field of the ICAT Facility Entity.

        Returns:
            Entity: The new ICAT Dataset Entity.
        """
        dataset_dict = dataset.excluded_dict()
        dataset_type = self.get_single_entity(
            entity="DatasetType",
            name=dataset.datasetType.name,
            facility_name=facility_name,
        )
        datafile_entities = []
        for datafile in dataset.datafiles:
            datafile_entity = self.new_datafile(datafile)
            datafile_entities.append(datafile_entity)

        return self.client.new(
            obj="Dataset",
            type=dataset_type,
            datafiles=datafile_entities,
            **dataset_dict,
        )

    def new_datafile(self, datafile: Datafile) -> Entity:
        """Creates a new ICAT Datafile Entity.

        Args:
            datafile (Datafile): Metadata for the Datafile to be created.

        Returns:
            Entity: The new ICAT Datafile Entity.
        """
        datafile_dict = datafile.excluded_dict()
        return self.client.new(obj="Datafile", **datafile_dict)

    def get_single_entity(
        self,
        entity: str,
        name: str,
        facility_name: str = None,
    ) -> Entity:
        """Returns the single ICAT Entity of type `entity` that matches the criteria.

        Args:
            entity (str): Type of entity to get, for example "Investigation".
            name (str): The value of the name field on the desired result.
            facility_name (str, optional):
                The name of the Facility. If unset, this will not form part of the
                query. Defaults to None.

        Raises:
            HTTPException: If no matching entities are found.

        Returns:
            Entity: The Entity matching the query.
        """
        conditions = {"name": f"={name!r}"}
        if facility_name is not None:
            conditions["facility.name"] = f"={facility_name!r}"

        query = Query(client=self.client, entity=entity, conditions=conditions)
        entities = self.client.search(query=query)

        if len(entities) == 0:
            detail = f"No {entity} with name {name}"
            raise HTTPException(status_code=400, detail=detail)
        else:
            return entities[0]

    @handle_icat_session
    def get_investigation_paths(self, investigation_ids: list[str]) -> list[str]:
        """Checks READ permissions for all the `investigation_ids` and builds paths to
        pass to FTS based on their fields in ICAT.

        Args:
            investigation_ids (list[str]): ICAT Investigation ids to generate paths for.

        Returns:
            list[str]: Paths to the Investigations in FTS.
        """
        query = Query(
            self.client,
            "Investigation",
            conditions={"id": f" IN ({str(investigation_ids)[1:-1]})"},
            includes=[
                "investigationInstruments.instrument",
                "investigationFacilityCycles.facilityCycle",
            ],
        )
        investigations = self.client.search(query=query)
        IcatClient.validate_entities(
            entities=investigations,
            expected_ids=investigation_ids,
        )

        paths = []
        for investigation in investigations:
            investigation_instrument = investigation.investigationInstruments[0]
            investigation_facility_cycle = investigation.investigationFacilityCycles[0]
            path = IcatClient.build_path(
                instrument_name=investigation_instrument.instrument.name,
                cycle_name=investigation_facility_cycle.facilityCycle.name,
                investigation_name=investigation.name,
                visit_id=investigation.visitId,
            )
            paths.append(path)
        return paths
