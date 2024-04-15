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
    ) -> set[str]:
        """Creates Investigations and child Datasets/Datafiles in ICAT and returns their
        paths for use with FTS.

        Args:
            investigations (list[Investigation]):
                Metadata for the Investigations to be created.

        Returns:
            set[str]: Paths to the Investigations in FTS.
        """
        beans = []
        all_paths = set()
        for investigation in investigations:
            entities, paths = self.new_investigation(investigation=investigation)
            beans.extend(entities)
            all_paths.update(paths)

        self.client.createMany(beans=beans)
        return all_paths

    def new_investigation(
        self,
        investigation: Investigation,
    ) -> tuple[list[Entity], set[str]]:
        """Create a new ICAT Investigation Entity (if needed) and child
        Dataset/Datafiles.

        Args:
            investigation (Investigation): Metadata for the Investigation to be created.

        Returns:
            tuple[list[Entity], set[str]]:
                The ICAT entities to be created and all the paths for the Datafiles.
        """
        dataset_entities = []
        all_paths = set()
        existing_investigation = self.get_single_entity(
            entity="Investigation",
            conditions={"name": investigation.name, "visitId": investigation.visitId},
            includes=[
                "investigationInstruments.instrument",
                "investigationFacilityCycles.facilityCycle",
            ],
            allow_empty=True,
        )

        for dataset in investigation.datasets:
            dataset_entity, paths = self.new_dataset(
                investigation=investigation,
                dataset=dataset,
                investigation_entity=existing_investigation,
            )
            dataset_entities.append(dataset_entity)
            all_paths.update(paths)

        if existing_investigation is not None:
            # Do not create the top level Investigation as it already exists
            # Just return the Datasets for creation
            return dataset_entities, all_paths
        else:
            new_investigation = self._new_investigation_entity(
                investigation=investigation,
                dataset_entities=dataset_entities,
            )
            return [new_investigation], all_paths

    def _new_investigation_entity(
        self,
        investigation: Investigation,
        dataset_entities: list[Entity],
    ) -> Entity:
        """Creates a new ICAT Investigation Entity.

        Args:
            investigation (Investigation): Metadata for the Investigation to be created.
            dataset_entities (list[Entity]):
                ICAT Dataset Entities belonging to the Investigation, also to be
                created.

        Returns:
            Entity: The new ICAT Investigation Entity.
        """
        investigation_dict = investigation.excluded_dict()

        # Get existing high level metadata
        facility = self.get_single_entity(
            entity="Facility",
            conditions={"name": investigation.facility.name},
        )
        investigation_type = self.get_single_entity(
            entity="InvestigationType",
            conditions={
                "name": investigation.investigationType.name,
                "facility.name": investigation.facility.name,
            },
        )
        facility_cycle = self.get_single_entity(
            entity="FacilityCycle",
            conditions={
                "name": investigation.facilityCycle.name,
                "facility.name": investigation.facility.name,
            },
        )
        instrument = self.get_single_entity(
            entity="Instrument",
            conditions={
                "name": investigation.instrument.name,
                "facility.name": investigation.facility.name,
            },
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

        return self.client.new(
            "Investigation",
            facility=facility,
            type=investigation_type,
            investigationFacilityCycles=[investigation_facility_cycle],
            investigationInstruments=[investigation_instrument],
            datasets=dataset_entities,
            **investigation_dict,
        )

    def new_dataset(
        self,
        investigation: Investigation,
        dataset: Dataset,
        investigation_entity: Entity | None,
    ) -> tuple[Entity, set[str]]:
        """Create a new ICAT Dataset Entity and child Datafiles.

        Args:
            investigation (Investigation): Metadata for the parent Investigation.
            dataset (Dataset): Metadata for the Dataset to be created.
            investigation_entity (Entity | None): Existing ICAT Investigation Entity.

        Returns:
            tuple[Entity, set[str]]:
                The new ICAT Dataset Entity and all the paths for Datafiles.
        """
        datafile_entities = []
        paths = set()
        for datafile in dataset.datafiles:
            datafile_entity, path = self.new_datafile(investigation, dataset, datafile)
            datafile_entities.append(datafile_entity)
            paths.add(path)

        dataset_type = self.get_single_entity(
            entity="DatasetType",
            conditions={
                "name": dataset.datasetType.name,
                "facility.name": investigation.facility.name,
            },
        )
        dataset_dict = dataset.excluded_dict()
        if investigation_entity is not None:
            dataset_dict["investigation"] = investigation_entity

        dataset_entity = self.client.new(
            obj="Dataset",
            type=dataset_type,
            datafiles=datafile_entities,
            **dataset_dict,
        )

        return dataset_entity, paths

    def new_datafile(
        self,
        investigation: Investigation,
        dataset: Dataset,
        datafile: Datafile,
    ) -> tuple[Entity, str]:
        """Creates a new ICAT Datafile Entity.

        Args:
            investigation (Investigation): Metadata for the parent Investigation.
            dataset (Dataset): Metadata for the parent Dataset.
            datafile (Datafile): Metadata for the Datafile to be created.

        Returns:
            tuple[Entity, str]: The new ICAT Datafile Entity and its path for FTS.
        """
        datafile_dict = datafile.excluded_dict()
        path = self.build_path(
            instrument_name=investigation.instrument.name,
            cycle_name=investigation.facilityCycle.name,
            investigation_name=investigation.name,
            visit_id=investigation.visitId,
            # TODO update when merged with the changes to build_path
        )
        return self.client.new(obj="Datafile", **datafile_dict), path

    def get_single_entity(
        self,
        entity: str,
        conditions: dict[str, str],
        includes: list[str] = None,
        allow_empty: bool = False,
    ) -> Entity | None:
        """Returns the single ICAT Entity of type `entity` that matches the criteria.

        Args:
            entity (str): Type of entity to get, for example "Investigation".
            conditions (dict[str, str]): Key value pairs for WHERE portion of the query.
            includes (list[str], optional): Attributes to INCLUDE. Defaults to None.
            allow_empty (bool, optional):
                If True, will return None rather than raise an exception if nothing
                matches the query. Defaults to False.

        Raises:
            HTTPException: If no matching entities are found and `allow_empty` is False.

        Returns:
            Entity | None: The Entity matching the query.
        """
        formatted_conditions = {}
        for key, value in conditions.items():
            formatted_conditions[key] = f"={value!r}"

        query = Query(
            client=self.client,
            entity=entity,
            conditions=formatted_conditions,
            includes=includes,
        )
        entities = self.client.search(query=query)

        if len(entities) == 0:
            if allow_empty:
                return None
            else:
                detail = f"No {entity} with conditions {conditions}"
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
