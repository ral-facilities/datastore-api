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
    def _build_entity_path(
        investigation: Entity,
        dataset: Entity,
        datafile: Entity,
    ) -> str:
        """Creates a deterministic path from ICAT entities.

        Args:
            investigation (Entity): ICAT Investigation.
            dataset (Entity): ICAT Dataset.
            datafile (Entity): ICAT Datfile.

        Returns:
            str: Path for FTS.
        """
        investigation_instrument = investigation.investigationInstruments[0]
        investigation_facility_cycle = investigation.investigationFacilityCycles[0]
        return IcatClient._build_path(
            instrument_name=investigation_instrument.instrument.name,
            cycle_name=investigation_facility_cycle.facilityCycle.name,
            investigation_name=investigation.name,
            visit_id=investigation.visitId,
            dataset_type_name=dataset.type.name,
            dataset_name=dataset.name,
            datafile_name=datafile.name,
        )

    @staticmethod
    def _build_path(
        instrument_name: str,
        cycle_name: str,
        investigation_name: str,
        visit_id: str,
        dataset_type_name: str,
        dataset_name: str,
        datafile_name: str,
    ) -> str:
        """Creates a deterministic path from ICAT metadata.

        Args:
            instrument_name (str): ICAT Instrument name.
            cycle_name (str): ICAT FacilityCycle name.
            investigation_name (str): ICAT Investigation name.
            visit_id (str): ICAT Investigation visitId.
            dataset_type_name (str): ICAT DatasetType name.
            dataset_name (str): ICAT Dataset name.
            datafile_name (str): ICAT Datafile name.

        Returns:
            str: Path for FTS.
        """
        return (
            f"{instrument_name}/{cycle_name}/{investigation_name}-{visit_id}/"
            f"{dataset_type_name}/{dataset_name}/{datafile_name}"
        )

    @staticmethod
    def _validate_entities(entities: EntityList, expected_ids: list[int]) -> None:
        """Check that the expected number of entities are returned from ICAT.

        Args:
            entities (EntityList): Entities returned from ICAT.
            expected_ids (list[int]): ICAT IDs that were requested.

        Raises:
            HTTPException: If the number of returned and expected entities differs.
        """
        if len(entities) != len(expected_ids):
            raise HTTPException(status_code=403, detail="insufficient permissions")

    @staticmethod
    def build_conditions(
        equals: dict[str, str] = None,
        contains: dict[str, str] = None,
    ) -> dict[str, str]:
        """Build the conditions dictionary for an ICAT query.

        Args:
            equals (dict[str, str], optional):
                Field with key should equal the value. Defaults to None.
            contains (dict[str, str], optional):
                Field with key should contain the value. Defaults to None.

        Returns:
            dict[str, str]: Formatted dictionary of ICAT query conditions.
        """
        formatted_conditions = {}
        if equals is not None:
            for key, value in equals.items():
                formatted_conditions[key] = f"={value!r}"

        if contains is not None:
            for key, value in contains.items():
                formatted_conditions[key] = f"LIKE '%{value}%'"

        return formatted_conditions

    @handle_icat_session
    def login(self, login_request: LoginRequest) -> str:
        """Uses the provided credentials to generate an ICAT sessionId.

        Args:
            login_request (LoginRequest):
                ICAT user credentials and authentication method.

        Returns:
            str: ICAT sessionId
        """
        return self.client.login(login_request.auth, login_request.credentials.dict())

    @handle_icat_session
    def login_functional(self) -> str:
        """Uses the functional credentials to generate an ICAT sessionId.

        Args:
            login_request (LoginRequest):
                ICAT user credentials and authentication method.

        Returns:
            str: ICAT sessionId
        """
        credentials = self.icat_settings.functional_user.dict(exclude={"auth"})
        return self.client.login(self.icat_settings.functional_user.auth, credentials)

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
    def create_many(
        self,
        beans: list[Entity],
    ) -> set[str]:
        return self.client.createMany(beans=beans)

    @handle_icat_session
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
        equals = {"name": investigation.name, "visitId": investigation.visitId}
        conditions = IcatClient.build_conditions(equals=equals)
        investigation_entity = self._get_single_entity(
            entity="Investigation",
            conditions=conditions,
            includes=[
                "investigationInstruments.instrument",
                "investigationFacilityCycles.facilityCycle",
            ],
            allow_empty=True,
        )

        if investigation_entity is None:
            investigation_entity = self._new_investigation_entity(
                investigation=investigation,
            )

        return investigation_entity

    def _new_investigation_entity(
        self,
        investigation: Investigation,
    ) -> Entity:
        """Creates a new ICAT Investigation Entity.

        Args:
            investigation (Investigation): Metadata for the Investigation to be created.

        Returns:
            Entity: The new ICAT Investigation Entity.
        """
        investigation_dict = investigation.excluded_dict()

        # Get existing high level metadata
        equals = {"name": investigation.facility.name}
        conditions = IcatClient.build_conditions(equals=equals)
        facility = self._get_single_entity(entity="Facility", conditions=conditions)

        equals = {
            "name": investigation.investigationType.name,
            "facility.name": investigation.facility.name,
        }
        conditions = IcatClient.build_conditions(equals=equals)
        investigation_type = self._get_single_entity(
            entity="InvestigationType",
            conditions=conditions,
        )

        equals = {
            "name": investigation.facilityCycle.name,
            "facility.name": investigation.facility.name,
        }
        conditions = IcatClient.build_conditions(equals=equals)
        facility_cycle = self._get_single_entity(
            entity="FacilityCycle",
            conditions=conditions,
        )

        equals = {
            "name": investigation.instrument.name,
            "facility.name": investigation.facility.name,
        }
        conditions = IcatClient.build_conditions(equals=equals)
        instrument = self._get_single_entity(entity="Instrument", conditions=conditions)

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
            **investigation_dict,
        )

    @handle_icat_session
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
            datafile_entity, path = self._new_datafile(investigation, dataset, datafile)
            datafile_entities.append(datafile_entity)
            paths.add(path)

        equals = {
            "name": dataset.datasetType.name,
            "facility.name": investigation.facility.name,
        }
        conditions = IcatClient.build_conditions(equals=equals)
        dataset_type = self._get_single_entity(
            entity="DatasetType",
            conditions=conditions,
        )
        dataset_dict = dataset.excluded_dict()
        if investigation_entity is not None:
            dataset_dict["investigation"] = investigation_entity

        dataset_parameter_entity_state = self.client.new(
            "DatasetParameter",
            type=self._get_parameter_type_state(investigation.facility.name),
            stringValue="SUBMITTED",
        )
        dataset_parameter_entity_jobs = self.client.new(
            "DatasetParameter",
            type=self._get_parameter_type_job_ids(investigation.facility.name),
            stringValue="",
        )
        dataset_entity = self.client.new(
            obj="Dataset",
            type=dataset_type,
            datafiles=datafile_entities,
            parameters=[dataset_parameter_entity_state, dataset_parameter_entity_jobs],
            **dataset_dict,
        )

        return dataset_entity, paths

    def _new_datafile(
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
        path = self._build_path(
            instrument_name=investigation.instrument.name,
            cycle_name=investigation.facilityCycle.name,
            investigation_name=investigation.name,
            visit_id=investigation.visitId,
            dataset_type_name=dataset.datasetType.name,
            dataset_name=dataset.name,
            datafile_name=datafile.name,
        )
        datafile_parameter_entity = self.client.new(
            "DatafileParameter",
            type=self._get_parameter_type_state(investigation.facility.name),
            stringValue="SUBMITTED",
        )
        datafile_entity = self.client.new(
            obj="Datafile",
            parameters=[datafile_parameter_entity],
            **datafile_dict,
        )
        return datafile_entity, path

    @handle_icat_session
    def get_single_entity(
        self,
        entity: str,
        conditions: dict[str, str],
        includes: list[str] = None,
        allow_empty: bool = False,
    ) -> Entity | None:
        return self._get_single_entity(
            entity=entity,
            conditions=conditions,
            includes=includes,
            allow_empty=allow_empty,
        )

    def _get_single_entity(
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
        query = Query(
            client=self.client,
            entity=entity,
            conditions=conditions,
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
    def get_paths(
        self,
        investigation_ids: list[str],
        dataset_ids: list[str],
        datafile_ids: list[str],
    ) -> set[str]:
        """Checks READ permissions for all the ids and builds paths to pass to FTS based
        on their fields in ICAT.

        Args:
            investigation_ids (list[str]): ICAT Investigation ids to generate paths for.
            dataset_ids (list[str]): ICAT Dataset ids to generate paths for.
            datafile_ids (list[str]): ICAT Datafile ids to generate paths for.

        Returns:
            set[str]: Paths to the data in FTS.
        """
        paths = self._get_investigation_paths(investigation_ids)
        paths.update(self._get_dataset_paths(dataset_ids))
        paths.update(self._get_datafile_paths(datafile_ids))
        return paths

    def _get_investigation_paths(self, investigation_ids: list[str]) -> set[str]:
        """Checks READ permissions for all the ids and builds paths to pass to FTS based
        on their fields in ICAT.

        Args:
            investigation_ids (list[str]): ICAT Investigation ids to generate paths for.

        Returns:
            set[str]: Paths to the data in FTS.
        """
        if not investigation_ids:
            return set()

        query = Query(
            self.client,
            "Investigation",
            conditions={"id": f" IN ({str(investigation_ids)[1:-1]})"},
            includes=[
                "investigationInstruments.instrument",
                "investigationFacilityCycles.facilityCycle",
                "datasets.type",
                "datasets.datafiles",
            ],
        )
        investigations = self.client.search(query=query)
        IcatClient._validate_entities(
            entities=investigations,
            expected_ids=investigation_ids,
        )

        paths = set()
        for investigation in investigations:
            for dataset in investigation.datasets:
                for datafile in dataset.datafiles:
                    path = IcatClient._build_entity_path(
                        investigation=investigation,
                        dataset=dataset,
                        datafile=datafile,
                    )
                    paths.add(path)

        return paths

    def _get_dataset_paths(self, dataset_ids: list[str]) -> set[str]:
        """Checks READ permissions for all the ids and builds paths to pass to FTS based
        on their fields in ICAT.

        Args:
            dataset_ids (list[str]): ICAT Dataset ids to generate paths for.

        Returns:
            set[str]: Paths to the data in FTS.
        """
        if not dataset_ids:
            return set()

        query = Query(
            self.client,
            "Dataset",
            conditions={"id": f" IN {dataset_ids}"},
            includes=[
                "investigation.investigationInstruments.instrument",
                "investigation.investigationFacilityCycles.facilityCycle",
                "type",
                "datafiles",
            ],
        )
        datasets = self.client.search(query=query)
        IcatClient._validate_entities(entities=datasets, expected_ids=dataset_ids)

        paths = set()
        for dataset in datasets:
            for datafile in dataset.datafiles:
                path = IcatClient._build_entity_path(
                    investigation=dataset.investigation,
                    dataset=dataset,
                    datafile=datafile,
                )
                paths.add(path)

        return paths

    def _get_datafile_paths(self, datafile_ids: list[str]) -> set[str]:
        """Checks READ permissions for all the ids and builds paths to pass to FTS based
        on their fields in ICAT.

        Args:
            datafile_ids (list[str]): ICAT Datafile ids to generate paths for.

        Returns:
            set[str]: Paths to the data in FTS.
        """
        if not datafile_ids:
            return set()

        query = Query(
            self.client,
            "Datafile",
            conditions={"id": f" IN {datafile_ids}"},
            includes=[
                "dataset.investigation.investigationInstruments.instrument",
                "dataset.investigation.investigationFacilityCycles.facilityCycle",
                "dataset.type",
            ],
        )
        datafiles = self.client.search(query=query)
        IcatClient._validate_entities(entities=datafiles, expected_ids=datafile_ids)

        paths = set()
        for datafile in datafiles:
            path = IcatClient._build_entity_path(
                investigation=datafile.dataset.investigation,
                dataset=datafile.dataset,
                datafile=datafile,
            )
            paths.add(path)

        return paths

    def _get_parameter_type_state(self, facility_name: str) -> Entity:
        """Get the ParameterType for recording FTS job state.

        Args:
            facility_name (str):
                Name attribute of the Facility the ParameterType belong to.

        Returns:
            Entity: ICAT ParameterType Entity for recording FTS state.
        """
        equals = {
            "name": self.icat_settings.parameter_type_job_state,
            "facility.name": facility_name,
            "units": "",
        }
        conditions = IcatClient.build_conditions(equals=equals)
        return self._get_single_entity(entity="ParameterType", conditions=conditions)

    def _get_parameter_type_job_ids(self, facility_name: str) -> Entity:
        """Get the ParameterType for recording FTS job ids.

        Args:
            facility_name (str):
                Name attribute of the Facility the ParameterType belong to.

        Returns:
            Entity: ICAT ParameterType Entity for recording FTS job ids.
        """
        equals = {
            "name": self.icat_settings.parameter_type_job_ids,
            "facility.name": facility_name,
            "units": "",
        }
        conditions = IcatClient.build_conditions(equals=equals)
        return self._get_single_entity(entity="ParameterType", conditions=conditions)

    @handle_icat_session
    def check_job_id(self, job_id: str) -> None:
        """Raises an error if the `job_id` appears in any of the active archival jobs.

        Args:
            job_id (str): FTS job_id to be cancelled.

        Raises:
            HTTPException: If the `job_id` appears in any of the active archival jobs.
        """
        equals = {"type.name": self.icat_settings.parameter_type_job_ids}
        contains = {"stringValue": job_id}
        conditions = IcatClient.build_conditions(equals=equals, contains=contains)
        parameter = self._get_single_entity(
            entity="DatasetParameter",
            conditions=conditions,
            allow_empty=True,
        )
        if parameter is not None:
            detail = "Archival jobs cannot be cancelled"
            raise HTTPException(status_code=400, detail=detail)
