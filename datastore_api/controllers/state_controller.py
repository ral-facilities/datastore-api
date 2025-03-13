from datetime import datetime

from icat.entity import Entity

from datastore_api.clients.fts3_client import get_fts3_client
from datastore_api.clients.icat_client import get_icat_cache, IcatClient
from datastore_api.controllers.state_counter import StateCounter
from datastore_api.models.dataset import (
    DatasetStatusListFilesResponse,
    DatasetStatusResponse,
)
from datastore_api.models.job import (
    ACTIVE_JOB_STATES,
    COMPLETE_TRANSFER_STATES,
)


class StateController:
    """Controller for the ICAT Parameters recording Dataset/Datafile state."""

    def __init__(self, session_id: str | None = None) -> None:
        """Initialise the controller with an optional ICAT session id.

        Args:
            session_id (str | None, optional):
                ICAT session id. If not provided, a functional login will be performed.
                Defaults to None.
        """
        self.icat_cache = get_icat_cache()
        self.icat_client = IcatClient()
        if session_id is not None:
            self.icat_client.client.sessionId = session_id
        else:
            self.icat_client.login_functional()

    @staticmethod
    def sum_completed_transfers(
        file_statuses: list[dict[str, str]] | dict[str, str],
    ) -> int:
        """Sum all the transfers in `file_statuses` that have completed.

        Args:
            file_statuses (list[dict[str, str]] | dict[str, str]):
                FTS transfer status for files.

        Returns:
            int: Total number of transfers in a terminal FTS state.
        """
        files_complete = 0
        if isinstance(file_statuses, dict):
            for value in file_statuses.values():
                if value in COMPLETE_TRANSFER_STATES:
                    files_complete += 1
        else:
            for file_status in file_statuses:
                if file_status["file_state"] in COMPLETE_TRANSFER_STATES:
                    files_complete += 1

        return files_complete

    def get_dataset_job_ids(self, dataset_id: int = None) -> list[Entity]:
        """Get ICAT DatasetParameters recording FTS job ids for a Dataset.

        Args:
            dataset_id (int, optional):
                ICAT Dataset id. If not provided, ids for all active jobs will be
                returned. Defaults to None.

        Returns:
            list[Entity]:
                List of ICAT DatasetParameter entities representing FTS job ids.
        """
        equals = {"type.name": self.icat_client.settings.parameter_type_job_ids}
        if dataset_id is not None:
            equals["dataset.id"] = dataset_id

        return self.icat_client.get_entities(
            entity="DatasetParameter",
            equals=equals,
            includes="1",
        )

    def get_dataset_state(self, dataset_id: int = None) -> Entity:
        """Get the ICAT DatasetParameter recording FTS state for a single Dataset.

        Args:
            dataset_id (int): ICAT Dataset id.

        Returns:
            list[Entity]: ICAT DatasetParameter representing FTS job state,
                dataset_id can be None it will return all the datasets
        """
        if dataset_id is not None:
            equals = {
                "type.name": self.icat_client.settings.parameter_type_job_state,
                "dataset.id": dataset_id,
            }
        else:
            equals = {
                "type.name": self.icat_client.settings.parameter_type_job_state,
            }

        return self.icat_client.get_entities(
            entity="DatasetParameter",
            equals=equals,
            includes="1",
        )

    def set_dataset_state(
        self,
        dataset_id: int,
        new_state: str,
        set_deletion_date: bool,
    ) -> None:
        """Explicitly set the state of a Dataset to `new_state`. Optionally,
        `set_deletion_date` to the current datetime.

        Args:
            dataset_id (int): ICAT Dataset id.
            new_state (str): Explicit term to record as the Dataset's state.
            set_deletion_date (bool):
                If `True`, the deletion date parameter will be set to the current
                datetime.
        """
        equals = {
            "type.name": self.icat_client.settings.parameter_type_job_state,
            "dataset.id": dataset_id,
        }
        self._set_parameter(
            parameter_entity_name="DatasetParameter",
            equals=equals,
            parent_id=dataset_id,
            parameter_type=self.icat_cache.parameter_type_job_state,
            string_value=new_state,
        )

        if set_deletion_date:
            equals = {
                "type.name": self.icat_client.settings.parameter_type_deletion_date,
                "dataset.id": dataset_id,
            }
            self._set_parameter(
                parameter_entity_name="DatasetParameter",
                equals=equals,
                parent_id=dataset_id,
                parameter_type=self.icat_cache.parameter_type_deletion_date,
                date_time_value=datetime.now(),
            )

    def get_datafile_states(self, dataset_id: int) -> list[Entity]:
        """Get ICAT DatafileParameters recording FTS states for all Datafiles belonging
        to this Dataset.

        Args:
            dataset_id (int): ICAT Dataset id.

        Returns:
            list[Entity]:
                ICAT DatafileParameter entities representing FTS transfer states.
        """
        equals = {
            "type.name": self.icat_client.settings.parameter_type_job_state,
            "datafile.dataset.id": dataset_id,
        }

        return self.icat_client.get_entities(
            entity="DatafileParameter",
            equals=equals,
            includes="1",
        )

    def set_datafile_states(
        self,
        dataset_id: int,
        new_state: str,
        set_deletion_date: bool,
    ) -> None:
        """Explicitly set the state of all Datafiles in a Dataset to `new_state`.
        Optionally, `set_deletion_date` to the current datetime.

        Args:
            dataset_id (int): ICAT Dataset id.
            new_state (str): Explicit term to record as the Datafiles' state.
            set_deletion_date (bool):
                If `True`, the deletion date parameter will be set to the current
                datetime.
        """
        equals = {
            "type.name": self.icat_client.settings.parameter_type_job_state,
            "datafile.dataset.id": dataset_id,
        }
        self._set_datafile_parameters(
            equals=equals,
            parameter_type=self.icat_cache.parameter_type_job_state,
            string_value=new_state,
        )

        if set_deletion_date:
            equals = {
                "type.name": self.icat_client.settings.parameter_type_deletion_date,
                "datafile.dataset.id": dataset_id,
            }
            self._set_datafile_parameters(
                equals=equals,
                parameter_type=self.icat_cache.parameter_type_deletion_date,
                date_time_value=datetime.now(),
            )

    def get_datafile_state(self, location: str) -> Entity:
        """Get ICAT Datafile Parameter recording FTS state for a single Datafile.

        Args:
            location (str): ICAT Datafile location.

        Returns:
            Entity: ICAT DatafileParameter representing FTS transfer state.
        """
        equals = {
            "type.name": self.icat_client.settings.parameter_type_job_state,
            "datafile.location": location.strip(),
        }

        return self.icat_client.get_single_entity(
            entity="DatafileParameter",
            equals=equals,
            includes="1",
        )

    def set_datafile_state(
        self,
        datafile_id: int,
        new_state: str,
        set_deletion_date: bool,
    ) -> None:
        """Explicitly set the state of a Datafile to `new_state`. Optionally,
        `set_deletion_date` to the current datetime.

        Args:
            datafile_id (int): ICAT Datafile id.
            new_state (str): Explicit term to record as the Datafile's state.
            set_deletion_date (bool):
                If `True`, the deletion date parameter will be set to the current
                datetime.
        """
        equals = {
            "type.name": self.icat_client.settings.parameter_type_job_state,
            "datafile.id": datafile_id,
        }
        self._set_parameter(
            parameter_entity_name="DatafileParameter",
            equals=equals,
            parent_id=datafile_id,
            parameter_type=self.icat_cache.parameter_type_job_state,
            string_value=new_state,
        )

        if set_deletion_date:
            equals = {
                "type.name": self.icat_client.settings.parameter_type_deletion_date,
                "datafile.id": datafile_id,
            }
            self._set_parameter(
                parameter_entity_name="DatafileParameter",
                equals=equals,
                parent_id=datafile_id,
                parameter_type=self.icat_cache.parameter_type_deletion_date,
                date_time_value=datetime.now(),
            )

    def _set_parameter(
        self,
        parameter_entity_name: str,
        equals: dict[str, str],
        parent_id: int,
        parameter_type: Entity,
        string_value: str = None,
        date_time_value: datetime = None,
    ) -> None:
        """Utility method to either create or update a `parameter_entity_name` of
        `parameter_type` with a new `string_value` or `date_time_value`.

        Args:
            parameter_entity_name (str):
                Either "DatasetParameter" or "DatafileParameter".
            equals (dict[str, str]): Query to identify the single Parameter to be set.
            parent_id (int): ICAT Entity id of the Parameter's parent.
            parameter_type (Entity): ICAT ParameterType Entity.
            string_value (str, optional): New Parameter.stringValue. Defaults to None.
            date_time_value (datetime, optional):
                New Parameter.dateTimeValue. Defaults to None.
        """
        parameter = self.icat_client.get_single_entity(
            entity=parameter_entity_name,
            equals=equals,
            includes="1",
            allow_empty=True,
        )
        if parameter is None:
            parent_entity_name = parameter_entity_name.replace("Parameter", "")
            parent = self.icat_client.get_single_entity(
                entity=parent_entity_name,
                equals={"id": parent_id},
            )
            parameter = self.icat_client.client.new(
                obj=parameter_entity_name,
                type=parameter_type,
                stringValue=string_value,
                dateTimeValue=date_time_value,
                **{parent_entity_name.lower(): parent},
            )
            self.icat_client.client.create(parameter)
            return

        parameter.stringValue = string_value
        parameter.dateTimeValue = date_time_value
        parameter.update()

    def _set_datafile_parameters(
        self,
        equals: dict[str, str],
        parameter_type: Entity,
        string_value: str = None,
        date_time_value: datetime = None,
    ) -> None:
        """Utility function to either create or update DatafileParameters of
        `parameter_type` with a new `string_value` or `date_time_value`.

        Args:
            equals (dict[str, str]): Query to identify the DatafileParameters to be set.
            parameter_type (Entity): ICAT ParameterType Entity.
            string_value (str, optional): _description_. Defaults to None.
            string_value (str, optional):
            New DatafileParameter.stringValue. Defaults to None.
            date_time_value (datetime, optional):
                New DatafileParameter.dateTimeValue. Defaults to None.
        """
        parameters = self.icat_client.get_entities(
            entity="DatafileParameter",
            equals=equals,
            includes="1",
        )
        if len(parameters) == 0:
            new_parameters = []
            datafiles = self.icat_client.get_entities(
                entity="Datafile",
                equals={"dataset.id": equals["datafile.dataset.id"]},
            )
            for datafile in datafiles:
                new_parameter = self.icat_client.client.new(
                    obj="DatafileParameter",
                    datafile=datafile,
                    type=parameter_type,
                    stringValue=string_value,
                    dateTimeValue=date_time_value,
                )
                new_parameters.append(new_parameter)
            self.icat_client.create_many(new_parameters)
            return

        for parameter in parameters:
            parameter.stringValue = string_value
            parameter.dateTimeValue = date_time_value
            parameter.update()

    def update_jobs(self, parameters: list[Entity]) -> list[StateCounter]:
        """Updates ICAT Parameter entities with the latest state information from FTS.

        Args:
            parameters (list[Entity]): DatasetParameter entities containing FTS job ids.

        Returns:
            list[StateCounter]: StateCounter for each DatasetParameter.
        """
        state_counters = []
        # parameter returns the Icat entity
        for parameter in parameters:
            state_counter = StateCounter()
            # check if the ICAT state is non terminal
            if parameter.stringValue in ACTIVE_JOB_STATES:
                # if not state_counter.check_state(state=parameter.stringValue):
                dataset_ids = parameter.dataset.id
                dataset_job_ids = self.get_dataset_job_ids(dataset_ids)
                for job_id in dataset_job_ids:
                    job_ids = job_id.stringValue.split(",")
                    statuses = get_fts3_client().statuses(
                        job_ids=job_ids,
                        list_files=True,
                    )
                    for status in statuses:
                        # check if the FTS state is non terminal
                        state_counter.check_state(
                            state=status["job_state"],
                            job_id=status["job_id"],
                        )
                        for file_status in status["files"]:
                            file_path, file_state = state_counter.check_file(
                                file_status=file_status,
                            )

                            datafile_status = self.get_datafile_state(
                                location=file_path,
                            )
                            if datafile_status.stringValue != file_state:
                                datafile_status.stringValue = file_state
                                self.icat_client.update(bean=datafile_status)

            if parameter.stringValue != state_counter.state:
                parameter.stringValue = state_counter.state
                self.icat_client.update(bean=parameter)

            state_counters.append(state_counter)

        return state_counters

    def get_dataset_status(
        self,
        dataset_id: int,
        list_files: bool = False,
    ) -> DatasetStatusResponse:
        """Get the status of a Dataset that may or may not have completed archival.

        Args:
            dataset_id (int): ICAT Dataset id.
            list_files (bool): Include state of individual files.

        Returns:
            DatasetStatusResponse: State of the Dataset (and Datafiles if relevant).
        """
        parameters = self.get_dataset_state(dataset_id=dataset_id)
        if parameters[0].stringValue in ACTIVE_JOB_STATES:
            state_controller_functional = StateController()
            return state_controller_functional._get_update_dataset_status(
                parameters=parameters,
                list_files=list_files,
            )
        else:
            return self._get_dataset_status(
                dataset_parameter=parameters[0],
                dataset_id=dataset_id,
                list_files=list_files,
            )

    def _get_update_dataset_status(
        self,
        parameters: list[Entity],
        list_files: bool,
    ) -> DatasetStatusResponse:
        """Get and update the status of a Dataset using the latest FTS information.

        Args:
            parameters (list[Entity]):
                List of a singe ICAT DatasetParameter representing the FTS job ids for a
                single Dataset.
            list_files (bool): Include state of individual files.

        Returns:
            DatasetStatusResponse: State of the Dataset (and Datafiles if relevant).
        """
        (state_counter,) = self.update_jobs(parameters)
        if list_files:
            return DatasetStatusListFilesResponse(
                state=state_counter.state,
                file_states=state_counter.file_states,
            )
        else:
            return DatasetStatusResponse(state=state_counter.state)

    def _get_dataset_status(
        self,
        dataset_parameter: Entity,
        dataset_id: int,
        list_files: bool,
    ) -> DatasetStatusResponse:
        """Get the status of a Dataset with completed archival.

        Args:
            dataset_id (int): ICAT Dataset id.
            list_files (bool): Include state of individual files.

        Returns:
            DatasetStatusResponse: State of the Dataset (and Datafiles if relevant).
        """
        print(dataset_parameter)
        dataset_parameter = self.get_dataset_state(dataset_id=dataset_id)
        state = dataset_parameter[0].stringValue
        if list_files:
            datafile_parameters = self.get_datafile_states(dataset_id=dataset_id)
            file_states = {}
            for parameter in datafile_parameters:
                file_states[parameter.datafile.location] = parameter.stringValue
            return DatasetStatusListFilesResponse(state=state, file_states=file_states)
        else:
            return DatasetStatusResponse(state=state)
