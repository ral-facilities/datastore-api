from functools import lru_cache
import logging

from fastapi import HTTPException
from icat import Client, ICATSessionError
from icat.entity import Entity, EntityList
from icat.query import Query

from datastore_api.config import get_settings, IcatUser
from datastore_api.models.archive import Datafile, Dataset, Investigation
from datastore_api.models.login import LoginRequest


LOGGER = logging.getLogger(__name__)


class IcatCache:
    """Holds a cache of static ICAT information for a particular Facility."""

    def __init__(self, facility_name: str) -> None:
        """Initialises a cache of static ICAT information for `facility_name`.

        Args:
            facility_name (str): Name of the Facility in ICAT.
        """
        icat_client = IcatClient()
        icat_client.login_functional()
        equals = {"facility.name": facility_name, "units": ""}
        self.parameter_type_job_state = icat_client.get_single_entity(
            entity="ParameterType",
            equals={"name": icat_client.settings.parameter_type_job_state, **equals},
        )
        self.parameter_type_job_ids = icat_client.get_single_entity(
            entity="ParameterType",
            equals={"name": icat_client.settings.parameter_type_job_ids, **equals},
        )


@lru_cache
def get_icat_cache(facility_name: str) -> IcatCache:
    return IcatCache(facility_name=facility_name)


class IcatClient:
    """Wrapper for ICAT functionality."""

    def __init__(self, session_id: str = None):
        """Initialise the Client with the provided `icat_settings`.

        Args:
            settings (IcatSettings): Settings for the ICAT client and admin users.
        """
        self.settings = get_settings().icat
        self.client = Client(self.settings.url, checkCert=self.settings.check_cert)
        self.client.autoLogout = False
        self.client.sessionId = session_id

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
    def _build_conditions(
        equals: dict[str, str] = None,
        contains: dict[str, str] = None,
        in_list: dict[str, list] = None,
    ) -> dict[str, str]:
        """Build the conditions dictionary for an ICAT query.

        Args:
            equals (dict[str, str], optional):
                Field with key should equal the value. Defaults to None.
            contains (dict[str, str], optional):
                Field with key should contain the value. Defaults to None.
            contains (dict[str, list], optional):
                Field with key should have value in list. Defaults to None.

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

        if in_list is not None:
            for key, value in in_list.items():
                formatted_conditions[key] = f" IN ({str(value)[1:-1]})"

        return formatted_conditions

    def login(self, login_request: LoginRequest) -> str:
        """Uses the provided credentials to generate and ICAT sessionId.

        Args:
            login_request (LoginRequest):
                ICAT user credentials and authentication method.

        Returns:
            str: ICAT sessionId
        """
        credentials = login_request.credentials.dict()
        return self._login(login_request.auth, credentials)

    def login_functional(self) -> str:
        """Uses the functional credentials to generate and ICAT sessionId.

        Args:
            login_request (LoginRequest):
                ICAT user credentials and authentication method.

        Returns:
            str: ICAT sessionId
        """
        credentials = self.settings.functional_user.dict(exclude={"auth"})
        return self._login(self.settings.functional_user.auth, credentials)

    def _login(self, auth: str, credentials: dict[str, str]) -> str:
        """Uses the provided credentials to generate and ICAT sessionId.

        Args:
            auth (str): Authentication method.
            credentials (dict[str, str]): ICAT user credentials.

        Raises:
            HTTPException: If credentials are not valid.

        Returns:
            str: ICAT sessionId for the logged in user.
        """
        try:
            session_id = self.client.login(auth, credentials)
            self.client.sessionId = session_id
            return session_id

        except ICATSessionError as e:
            raise HTTPException(status_code=401, detail=e.message) from e

    def authorise_admin(self) -> None:
        """Checks the sessionId belonging to the current user is for an admin.

        Raises:
            HTTPException: If the user is not one of the configured admin users.
        """
        user = self.client.getUserName()
        auth, username = user.split("/")
        if IcatUser(auth=auth, username=username) not in self.settings.admin_users:
            raise HTTPException(status_code=403, detail="insufficient permissions")

    def create_many(
        self,
        beans: list[Entity],
    ) -> set[str]:
        """Creates multiple ICAT entities.

        Args:
            beans (list[Entity]): ICAT entities to create.

        Returns:
            set[str]: Ids of created entities.
        """
        LOGGER.debug("Calling createMany with %s beans", len(beans))
        return self.client.createMany(beans=beans)

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
        investigation_entity = self.get_single_entity(
            entity="Investigation",
            equals=equals,
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
            dataset_entities (list[Entity]):
                ICAT Dataset Entities belonging to the Investigation, also to be
                created.

        Returns:
            Entity: The new ICAT Investigation Entity.
        """
        investigation_dict = investigation.excluded_dict()

        # Get existing high level metadata
        equals = {"name": investigation.facility.name}
        facility = self.get_single_entity(entity="Facility", equals=equals)

        equals = {
            "name": investigation.investigationType.name,
            "facility.name": investigation.facility.name,
        }
        investigation_type = self.get_single_entity(
            entity="InvestigationType",
            equals=equals,
        )

        equals = {
            "name": investigation.facilityCycle.name,
            "facility.name": investigation.facility.name,
        }
        facility_cycle = self.get_single_entity(
            entity="FacilityCycle",
            equals=equals,
        )

        equals = {
            "name": investigation.instrument.name,
            "facility.name": investigation.facility.name,
        }
        instrument = self.get_single_entity(entity="Instrument", equals=equals)

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

    def new_dataset(
        self,
        investigation: Investigation,
        dataset: Dataset,
        investigation_entity: Entity,
    ) -> tuple[Entity, set[str]]:
        """Create a new ICAT Dataset Entity and child Datafiles.

        Args:
            investigation (Investigation): Metadata for the parent Investigation.
            dataset (Dataset): Metadata for the Dataset to be created.
            investigation_entity (Entity): Existing or new ICAT Investigation Entity.

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
        dataset_type = self.get_single_entity(
            entity="DatasetType",
            equals=equals,
        )
        dataset_dict = dataset.excluded_dict()
        if investigation_entity.id is not None:
            dataset_dict["investigation"] = investigation_entity

        icat_cache = get_icat_cache(facility_name=investigation.facility.name)
        dataset_parameter_entity_state = self.client.new(
            "DatasetParameter",
            type=icat_cache.parameter_type_job_state,
            stringValue="SUBMITTED",
        )
        dataset_parameter_entity_jobs = self.client.new(
            "DatasetParameter",
            type=icat_cache.parameter_type_job_ids,
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
        icat_cache = get_icat_cache(facility_name=investigation.facility.name)
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
            type=icat_cache.parameter_type_job_state,
            stringValue="SUBMITTED",
        )
        datafile_entity = self.client.new(
            obj="Datafile",
            location=path,
            parameters=[datafile_parameter_entity],
            **datafile_dict,
        )
        return datafile_entity, path

    def update(self, bean: Entity) -> None:
        """Updates `bean` with changes to its attributes.

        Args:
            bean (Entity): ICAT Entity with modified attributes.
        """
        self.client.update(bean)

    def delete_many(self, beans: list[Entity]) -> None:
        """Deletes `beans`.

        Args:
            beans (list[Entity]): ICAT entities to be deleted.
        """
        self.client.deleteMany(beans)

    def get_entities(
        self,
        entity: str,
        equals: dict[str, str] = None,
        contains: dict[str, str] = None,
        includes: list[str] = None,
    ) -> list[Entity]:
        """Returns all ICAT entities matching the criteria.

        Args:
            entity (str): Type of entity to get, for example "Investigation".
            equals (dict[str, str], optional):
                Key value pairs where the attribute should equal the value.
                Defaults to None.
            contains (dict[str, str], optional):
                Key value pairs where the attribute should contain the value.
                Defaults to None.
            includes (list[str], optional): Attributes to INCLUDE. Defaults to None.

        Returns:
            list[Entity]: The entities matching the query.
        """
        conditions = IcatClient._build_conditions(equals=equals, contains=contains)
        query = Query(
            client=self.client,
            entity=entity,
            conditions=conditions,
            includes=includes,
        )
        return self.client.search(query=str(query))

    def get_single_entity(
        self,
        entity: str,
        equals: dict[str, str] = None,
        contains: dict[str, str] = None,
        includes: list[str] = None,
        allow_empty: bool = False,
    ) -> Entity | None:
        """Returns the single ICAT Entity of type `entity` that matches the criteria.

        Args:
            entity (str): Type of entity to get, for example "Investigation".
            equals (dict[str, str], optional):
                Key value pairs where the attribute should equal the value.
                Defaults to None.
            contains (dict[str, str], optional):
                Key value pairs where the attribute should contain the value.
                Defaults to None.
            includes (list[str], optional): Attributes to INCLUDE. Defaults to None.
            allow_empty (bool, optional):
                If True, will return None rather than raise an exception if nothing
                matches the query. Defaults to False.

        Raises:
            HTTPException: If no matching entities are found and `allow_empty` is False.

        Returns:
            Entity | None: The Entity matching the query.
        """
        entities = self.get_entities(entity, equals, contains, includes)

        if len(entities) == 0:
            if allow_empty:
                return None
            else:
                detail = f"No {entity} with {equals} and fields containing {contains}"
                raise HTTPException(status_code=400, detail=detail)
        else:
            return entities[0]

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
            conditions=IcatClient._build_conditions(in_list={"id": investigation_ids}),
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
            conditions=IcatClient._build_conditions(in_list={"id": dataset_ids}),
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
            conditions=IcatClient._build_conditions(in_list={"id": datafile_ids}),
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

    def check_job_id(self, job_id: str) -> None:
        """Raises an error if the `job_id` appears in any of the active archival jobs.

        Args:
            job_id (str): FTS job_id to be cancelled.

        Raises:
            HTTPException: If the `job_id` appears in any of the active archival jobs.
        """
        parameter = self.get_single_entity(
            entity="DatasetParameter",
            equals={"type.name": self.settings.parameter_type_job_ids},
            contains={"stringValue": job_id},
            allow_empty=True,
        )
        if parameter is not None:
            detail = "Archival jobs cannot be cancelled"
            raise HTTPException(status_code=400, detail=detail)
