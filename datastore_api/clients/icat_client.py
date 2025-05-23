from functools import lru_cache
import logging

from fastapi import HTTPException
from icat import Client, ICATSessionError
from icat.entity import Entity, EntityList
from icat.query import Query

from datastore_api.config import get_settings, IcatUser
from datastore_api.models.icat import (
    Datafile,
    Dataset,
    InstrumentIdentifier,
    Investigation,
    InvestigationIdentifier,
    Parameter,
    Sample,
    TechniqueIdentifier,
)
from datastore_api.models.login import LoginRequest


LOGGER = logging.getLogger(__name__)


class IcatCache:
    """Holds a cache of static ICAT information for a particular Facility."""

    def __init__(self) -> None:
        """Initialises a cache of static ICAT information."""
        icat_client = IcatClient()
        icat_client.login_functional()
        equals = {"facility.name": icat_client.settings.facility_name, "units": ""}
        facility = icat_client.get_single_entity(
            entity="Facility",
            equals={"name": icat_client.settings.facility_name},
        )

        self.parameter_type_job_state = icat_client.get_single_entity(
            entity="ParameterType",
            equals={"name": icat_client.settings.parameter_type_job_state, **equals},
            allow_empty=icat_client.settings.create_parameter_types,
        )
        if self.parameter_type_job_state is None:
            self.parameter_type_job_state = icat_client.client.new(
                obj="ParameterType",
                name=icat_client.settings.parameter_type_job_state,
                units="",
                valueType="STRING",
                applicableToDataset=True,
                applicableToDatafile=True,
                facility=facility,
            )
            icat_entity_id = icat_client.client.create(self.parameter_type_job_state)
            self.parameter_type_job_state.id = icat_entity_id

        self.parameter_type_job_ids = icat_client.get_single_entity(
            entity="ParameterType",
            equals={"name": icat_client.settings.parameter_type_job_ids, **equals},
            allow_empty=icat_client.settings.create_parameter_types,
        )
        if self.parameter_type_job_ids is None:
            self.parameter_type_job_ids = icat_client.client.new(
                obj="ParameterType",
                name=icat_client.settings.parameter_type_job_ids,
                units="",
                valueType="STRING",
                applicableToDataset=True,
                facility=facility,
            )
            icat_entity_id = icat_client.client.create(self.parameter_type_job_ids)
            self.parameter_type_job_state.id = icat_entity_id

        name = icat_client.settings.parameter_type_deletion_date
        self.parameter_type_deletion_date = icat_client.get_single_entity(
            entity="ParameterType",
            equals={"name": name, **equals},
            allow_empty=icat_client.settings.create_parameter_types,
        )
        if self.parameter_type_deletion_date is None:
            self.parameter_type_deletion_date = icat_client.client.new(
                obj="ParameterType",
                name=icat_client.settings.parameter_type_deletion_date,
                units="",
                valueType="STRING",
                applicableToDataset=True,
                applicableToDatafile=True,
                facility=facility,
            )
            icat_entity_id = icat_client.client.create(
                self.parameter_type_deletion_date,
            )
            self.parameter_type_job_state.id = icat_entity_id


@lru_cache
def get_icat_cache() -> IcatCache:
    return IcatCache()


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
        """Uses the provided credentials to generate an ICAT sessionId.

        Args:
            login_request (LoginRequest):
                ICAT user credentials and authentication method.

        Returns:
            str: ICAT sessionId
        """
        credentials = login_request.credentials.model_dump()
        credentials["password"] = login_request.credentials.password.get_secret_value()
        return self._login(login_request.auth, credentials)

    def login_functional(self) -> str:
        """Uses the functional credentials to generate an ICAT sessionId.

        Args:
            login_request (LoginRequest):
                ICAT user credentials and authentication method.

        Returns:
            str: ICAT sessionId
        """
        functional_user = self.settings.functional_user
        credentials = functional_user.model_dump(exclude={"auth"})
        credentials["password"] = functional_user.password.get_secret_value()
        return self._login(functional_user.auth, credentials)

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
        investigation: Investigation | InvestigationIdentifier,
    ) -> tuple[list[Entity], set[str]]:
        """Create a new ICAT Investigation Entity (if needed) and child
        Dataset/Datafiles.

        Args:
            investigation (Investigation | InvestigationIdentifier):
                Either full or identifying metadata for the Investigation to be created.

        Returns:
            Entity: The ICAT Investigation entity to be created.
        """
        equals = {
            "name": investigation.name,
            "visitId": investigation.visitId,
            "facility.name": self.settings.facility_name,
        }
        investigation_entity = self.get_single_entity(
            entity="Investigation",
            equals=equals,
            includes=[
                "investigationInstruments.instrument",
                "investigationFacilityCycles.facilityCycle",
            ],
            allow_empty=isinstance(investigation, Investigation),
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
        equals = {"name": self.settings.facility_name}
        facility = self.get_single_entity(entity="Facility", equals=equals)

        equals = {
            "name": investigation.investigationType.name,
            "facility.name": self.settings.facility_name,
        }
        investigation_type = self.get_single_entity(
            entity="InvestigationType",
            equals=equals,
        )

        equals = {
            "name": investigation.facilityCycle.name,
            "facility.name": self.settings.facility_name,
        }
        facility_cycle = self.get_single_entity(
            entity="FacilityCycle",
            equals=equals,
        )

        equals = {
            "name": investigation.instrument.name,
            "facility.name": self.settings.facility_name,
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
        dataset: Dataset,
        investigation_entity: Entity,
    ) -> Entity:
        """Create a new ICAT Dataset Entity and child Datafiles.

        Args:
            dataset (Dataset): Metadata for the Dataset to be created.
            investigation_entity (Entity): Existing or new ICAT Investigation Entity.

        Returns:
            Entity: The new ICAT Dataset Entity.
        """
        datafile_entities = []
        paths = set()
        for datafile in dataset.datafiles:
            datafile_entity = self._new_datafile(datafile)
            datafile_entities.append(datafile_entity)
            paths.add(datafile.location)

        dataset_dict = dataset.excluded_dict()
        if investigation_entity.id is not None:
            dataset_dict["investigation"] = investigation_entity

        dataset_type = self.get_single_entity(
            entity="DatasetType",
            equals={
                "name": dataset.datasetType.name,
                "facility.name": self.settings.facility_name,
            },
        )

        parameters = self._extract_parameters(
            "Dataset",
            dataset.parameters,
        )
        icat_cache = get_icat_cache()
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
        parameters.append(dataset_parameter_entity_state)
        parameters.append(dataset_parameter_entity_jobs)

        dataset_dict["datasetTechniques"] = self._extract_techniques(
            techniques=dataset.datasetTechniques,
        )
        dataset_dict["datasetInstruments"] = self._extract_instruments(
            instruments=dataset.datasetInstruments,
        )
        if dataset.sample is not None:
            sample = self._extract_sample(
                investigation_entity,
                dataset.sample,
            )

            dataset_dict["sample"] = sample

        dataset_entity = self.client.new(
            obj="Dataset",
            type=dataset_type,
            datafiles=datafile_entities,
            parameters=parameters,
            **dataset_dict,
        )

        return dataset_entity

    def _extract_instruments(
        self,
        instruments: list[InstrumentIdentifier],
    ) -> list[Entity]:
        """Instantiate, but do not persist, new DatasetInstrument entities for the
        provided metadata.

        Args:
            instruments (list[InstrumentIdentifier]):
                Identifying metadata for ICAT Instrument entities.

        Returns:
            list[Entity]: New ICAT DatasetInstrument entities with instrument set.
        """
        instrument_entities = []
        for instrument in instruments:
            equals = {
                "name": instrument.name,
                "facility.name": self.settings.facility_name,
            }
            instrument_entity = self.get_single_entity("Instrument", equals=equals)
            dataset_instrument = self.client.new(
                "DatasetInstrument",
                instrument=instrument_entity,
            )
            instrument_entities.append(dataset_instrument)

        return instrument_entities

    def _extract_techniques(
        self,
        techniques: list[TechniqueIdentifier],
    ) -> list[Entity]:
        """Instantiate, but do not persist, new DatasetTechnique entities for the
        provided metadata.

        Args:
            techniques (list[TechniqueIdentifier]):
                Identifying metadata for ICAT Technique entities.

        Returns:
            list[Entity]: New ICAT DatasetTechnique entities with technique set.
        """
        technique_entities = []
        for technique in techniques:
            equals = {"name": technique.name}
            technique_entity = self.get_single_entity("Technique", equals=equals)
            dataset_technique = self.client.new(
                "DatasetTechnique",
                technique=technique_entity,
            )
            technique_entities.append(dataset_technique)

        return technique_entities

    def _extract_sample(
        self,
        investigation_entity: Entity,
        sample: Sample,
    ) -> Entity:
        """Instantiate and persist new Sample entity for the provided metadata. The
        Sample must be associated with the Investigation, and cannot be persisted by the
        creation of a Dataset so this is done now.

        Args:
            investigation_entity (Entity):
                ICAT Investigation entity to associate the sample with.
            sample (Sample): Full metadata for the ICAT Sample to be created.

        Returns:
            Entity: Created and persisted ICAT Sample entity.
        """
        equals = {
            "name": sample.name,
            "investigation.name": str(investigation_entity.name),
            "investigation.visitId": str(investigation_entity.visitId),
            "investigation.facility.name": self.settings.facility_name,
        }
        sample_entity = self.get_single_entity(
            "Sample",
            equals=equals,
            allow_empty=True,
        )
        if sample_entity is not None:
            return sample_entity

        equals = {
            "name": sample.sample_type.name,
            "molecularFormula": sample.sample_type.molecularFormula,
            "facility.name": self.settings.facility_name,
        }
        sample_type = self.get_single_entity("SampleType", equals=equals)
        parameters = self._extract_parameters(
            parent="Sample",
            parameters=sample.parameters,
        )
        sample_entity = self.client.new(
            "Sample",
            type=sample_type,
            investigation=investigation_entity,
            parameters=parameters,
            **sample.excluded_dict(),
        )
        sample_entity.id = self.client.create(sample_entity)

        return sample_entity

    def _new_datafile(
        self,
        datafile: Datafile,
    ) -> Entity:
        """Creates a new ICAT Datafile Entity.

        Args:
            datafile (Datafile): Metadata for the Datafile to be created.

        Returns:
            Entity: The new ICAT Datafile Entity.
        """
        datafile_dict = datafile.excluded_dict()

        parameters = self._extract_parameters(
            "Datafile",
            datafile.parameters,
        )
        icat_cache = get_icat_cache()
        datafile_parameter_entity = self.client.new(
            "DatafileParameter",
            type=icat_cache.parameter_type_job_state,
            stringValue="SUBMITTED",
        )
        parameters.append(datafile_parameter_entity)

        if datafile.datafileFormat is not None:
            equals = {
                "name": datafile.datafileFormat.name,
                "version": datafile.datafileFormat.version,
                "facility.name": self.settings.facility_name,
            }
            datafile_format = self.get_single_entity("DatafileFormat", equals=equals)
            datafile_dict["datafileFormat"] = datafile_format

        datafile_entity = self.client.new(
            obj="Datafile",
            parameters=parameters,
            **datafile_dict,
        )
        return datafile_entity

    def _extract_parameters(
        self,
        parent: str,
        parameters: list[Parameter],
    ) -> list[Entity]:
        """Instantiate, but do not persist, new Parameter entities for the provided
        metadata.

        Args:
            parent (str): The entity type the `parameters` belong to, e.g. "Dataset".
            parameters (list[Parameter]): Full metadata for ICAT Parameter entities.

        Returns:
            list[Entity]: New ICAT Parameter entities with type set.
        """
        parameter_entities = []
        for parameter in parameters:
            equals = {
                "name": parameter.parameter_type.name,
                "units": parameter.parameter_type.units,
                "facility.name": self.settings.facility_name,
            }
            parameter_type = self.get_single_entity("ParameterType", equals=equals)
            parameter_entity = self.client.new(
                f"{parent}Parameter",
                type=parameter_type,
                **parameter.excluded_dict(),
            )
            parameter_entities.append(parameter_entity)

        return parameter_entities

    def update(self, bean: Entity) -> None:
        """Updates `bean` with changes to its attributes.

        Args:
            bean (Entity): ICAT Entity with modified attributes.
        """
        LOGGER.debug("Updating %s", bean)
        self.client.update(bean)

    def delete_many(self, beans: list[Entity]) -> None:
        """Deletes `beans`.

        Args:
            beans (list[Entity]): ICAT entities to be deleted.
        """
        LOGGER.debug("Deleting %s", beans)
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

    def get_unique_datafiles(
        self,
        investigation_ids: set[str],
        dataset_ids: set[str],
        datafile_ids: set[str],
    ) -> list[Entity]:
        """Checks READ permissions for all the ids and builds paths to pass to FTS based
        on their fields in ICAT.

        Args:
            investigation_ids (list[str]): ICAT Investigation ids to generate paths for.
            dataset_ids (list[str]): ICAT Dataset ids to generate paths for.
            datafile_ids (list[str]): ICAT Datafile ids to generate paths for.

        Returns:
            set[str]: Paths to the data in FTS.
        """
        datafiles = self._get_investigation_paths(investigation_ids)
        datafiles.extend(self._get_dataset_paths(investigation_ids, dataset_ids))
        datafiles.extend(
            self._get_datafile_paths(investigation_ids, dataset_ids, datafile_ids),
        )
        return datafiles

    def _get_investigation_paths(self, investigation_ids: set[str]) -> list[Entity]:
        """Checks READ permissions for all the ids and builds paths to pass to FTS based
        on their fields in ICAT.

        Args:
            investigation_ids (set[str]): ICAT Investigation ids to generate paths for.

        Returns:
            list[Entity]: Datafiles to be transferred.
        """
        if not investigation_ids:
            return []

        query = Query(
            self.client,
            "Investigation",
            conditions=IcatClient._build_conditions(in_list={"id": investigation_ids}),
            includes=["datasets.datafiles"],
        )
        investigations = self.client.search(query=query)
        IcatClient._validate_entities(
            entities=investigations,
            expected_ids=investigation_ids,
        )

        datafiles = []
        for investigation in investigations:
            for dataset in investigation.datasets:
                datafiles.extend(dataset.datafiles)

        return datafiles

    def _get_dataset_paths(
        self,
        investigation_ids: set[str],
        dataset_ids: set[str],
    ) -> list[Entity]:
        """Checks READ permissions for all the ids and builds paths to pass to FTS based
        on their fields in ICAT.

        Args:
            investigation_ids (set[str]):
                ICAT Investigation ids that have already been accounted for.
                Datasets belonging to these Investigations will be skipped.
            dataset_ids (set[str]): ICAT Dataset ids to generate paths for.

        Returns:
            list[Entity]: Datafiles to be transferred.
        """
        if not dataset_ids:
            return set()

        query = Query(
            self.client,
            "Dataset",
            conditions=IcatClient._build_conditions(in_list={"id": dataset_ids}),
            includes=["investigation", "datafiles"],
        )
        datasets = self.client.search(query=query)
        IcatClient._validate_entities(entities=datasets, expected_ids=dataset_ids)

        datafiles = []
        for dataset in datasets:
            if dataset.investigation.id not in investigation_ids:
                datafiles.extend(dataset.datafiles)

        return datafiles

    def _get_datafile_paths(
        self,
        investigation_ids: set[str],
        dataset_ids: set[str],
        datafile_ids: list[str],
    ) -> list[Entity]:
        """Checks READ permissions for all the ids and builds paths to pass to FTS based
        on their fields in ICAT.

        Args:
            investigation_ids (set[str]):
                ICAT Investigation ids that have already been accounted for.
                Datafiles belonging to these Investigations will be skipped.
            dataset_ids (set[str]):
                ICAT Dataset ids that have already been accounted for.
                Datafiles belonging to these Datasets will be skipped.
            datafile_ids (list[str]): ICAT Datafile ids to generate paths for.

        Returns:
            list[Entity]: Datafiles to be transferred.
        """
        if not datafile_ids:
            return set()

        query = Query(
            self.client,
            "Datafile",
            conditions=IcatClient._build_conditions(in_list={"id": datafile_ids}),
            includes=["dataset.investigation"],
        )
        all_datafiles = self.client.search(query=query)
        IcatClient._validate_entities(entities=all_datafiles, expected_ids=datafile_ids)

        datafiles = []
        for datafile in all_datafiles:
            if datafile.dataset.id not in dataset_ids:
                if datafile.dataset.investigation.id not in investigation_ids:
                    datafiles.append(datafile)

        return datafiles

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
