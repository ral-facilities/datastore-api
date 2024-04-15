from functools import wraps
from typing import Any, Callable

from fastapi import HTTPException
from icat import Client, ICATSessionError
from icat.entity import Entity, EntityList
from icat.query import Query

from datastore_api.config import IcatSettings, IcatUser
from datastore_api.models.archive import Investigation
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
    def build_investigation_path(
        instrument_name: str,
        cycle_name: str,
        investigation_name: str,
        visit_id: str,
    ) -> str:
        """Creates a deterministic path from Investigation metadata.

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
    def build_entity_path(
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
        investigation_facility_cycle = investigation.investigationFacilityCycle[0]
        return IcatClient.build_path(
            instrument_name=investigation_instrument.instrument.name,
            cycle_name=investigation_facility_cycle.facilityCycle.name,
            investigation_name=investigation.name,
            visit_id=investigation.visitId,
            dataset_type_name=dataset.type.name,
            dataset_name=dataset.name,
            datafile_name=datafile.name,
        )

    @staticmethod
    def build_path(
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
            f"/{instrument_name}/{cycle_name}/{investigation_name}-{visit_id}"
            f"/{dataset_type_name}/{dataset_name}/{datafile_name}"
        )

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
    def create_investigations(
        self,
        investigations: list[Investigation],
    ) -> list[str]:
        """Creates Investigations in ICAT and returns their paths for use with FTS.

        Args:
            investigations (list[Investigation]):
                Metadata for the Investigations to be created.

        Returns:
            list[str]: Paths to the Investigations in FTS.
        """
        beans = []
        paths = []
        for investigation in investigations:
            exclude = {"facility", "type", "instrument", "cycle"}
            investigation_dict = investigation.dict(exclude=exclude, exclude_none=True)
            entity = self.client.new("Investigation", **investigation_dict)
            beans.append(entity)

            path = IcatClient.build_investigation_path(
                instrument_name=investigation.instrument.name,
                cycle_name=investigation.cycle.name,
                investigation_name=investigation.name,
                visit_id=investigation.visitId,
            )
            paths.append(path)

        self.client.createMany(beans=beans)
        return paths

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
        query = Query(
            self.client,
            "Investigation",
            conditions={"id": f" IN {investigation_ids}"},
            includes=[
                "InvestigationInstrument",
                "Instrument",
                "InvestigationFacilityCycle",
                "FacilityCycle",
                "Dataset",
                "DatasetType",
                "Datafile",
            ],
        )
        investigations = self.client.search(query=query)
        IcatClient.validate_entities(
            entities=investigations,
            expected_ids=investigation_ids,
        )

        paths = set()
        for investigation in investigations:
            path = IcatClient.build_entity_path(
                investigation=investigation,
                dataset=investigation.dataset,
                datafile=investigation.dataset.datafile,
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
        query = Query(
            self.client,
            "Dataset",
            conditions={"id": f" IN {dataset_ids}"},
            includes=[
                "Investigation",
                "InvestigationInstrument",
                "Instrument",
                "InvestigationFacilityCycle",
                "FacilityCycle",
                "Datafile",
            ],
        )
        datasets = self.client.search(query=query)
        IcatClient.validate_entities(entities=datasets, expected_ids=dataset_ids)

        paths = set()
        for dataset in datasets:
            path = IcatClient.build_entity_path(
                investigation=dataset.investigation,
                dataset=dataset,
                datafile=dataset.datafile,
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
        query = Query(
            self.client,
            "Datafile",
            conditions={"id": f" IN {datafile_ids}"},
            includes=[
                "Dataset",
                "DatasetType",
                "Investigation",
                "InvestigationInstrument",
                "Instrument",
                "InvestigationFacilityCycle",
                "FacilityCycle",
            ],
        )
        datafiles = self.client.search(query=query)
        IcatClient.validate_entities(entities=datafiles, expected_ids=datafile_ids)

        paths = set()
        for datafile in datafiles:
            path = IcatClient.build_entity_path(
                investigation=datafile.dataset.investigation,
                dataset=datafile.dataset,
                datafile=datafile,
            )
            paths.add(path)

        return paths
