from icat.entity import Entity
from pydantic_core import Url

from datastore_api.fts3_client import get_fts3_client
from datastore_api.icat_client import IcatClient
from datastore_api.models.dataset import (
    DatasetStatusListFilesResponse,
    DatasetStatusResponse,
)
from datastore_api.state_counter import StateCounter


class StateController:
    """Controller for the ICAT Parameters recording Dataset/Datafile state."""

    def __init__(self, session_id: str | None = None) -> None:
        """Initialise the controller with an optional ICAT session id.

        Args:
            session_id (str | None, optional):
                ICAT session id. If not provided, a functional login will be performed.
                Defaults to None.
        """
        self.icat_client = IcatClient()
        if session_id is not None:
            self.icat_client.client.sessionId = session_id
        else:
            self.icat_client.login_functional()

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

    def get_dataset_state(self, dataset_id: int) -> Entity:
        """Get the ICAT DatasetParameter recording FTS state for a single Dataset.

        Args:
            dataset_id (int): ICAT Dataset id.

        Returns:
            Entity: ICAT DatasetParameter representing FTS job state.
        """
        equals = {
            "type.name": self.icat_client.settings.parameter_type_job_state,
            "dataset.id": dataset_id,
        }

        return self.icat_client.get_single_entity(
            entity="DatasetParameter",
            equals=equals,
            includes="1",
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

    def get_datafile_state(self, location: str) -> Entity:
        """Get ICAT Datafile Parameter recording FTS state for a single Datafile.

        Args:
            location (str): ICAT Datafile location.

        Returns:
            Entity: ICAT DatafileParameter representing FTS transfer state.
        """
        equals = {
            "type.name": self.icat_client.settings.parameter_type_job_state,
            "datafile.location": location,
        }

        return self.icat_client.get_single_entity(
            entity="DatafileParameter",
            equals=equals,
            includes="1",
        )

    def update_jobs(self, parameters: list[Entity]) -> list[StateCounter]:
        """Updates ICAT Parameter entities with the latest state information from FTS.

        Args:
            parameters (list[Entity]): DatasetParameter entities containing FTS job ids.

        Returns:
            list[StateCounter]: StateCounter for each DatasetParameter.
        """
        beans_to_delete = []
        state_counters = []
        for parameter in parameters:
            state_counter = StateCounter()
            job_ids = parameter.stringValue.split(",")
            statuses = get_fts3_client().statuses(job_ids=job_ids, list_files=True)
            for status in statuses:
                state_counter.check_state(
                    state=status["job_state"],
                    job_id=status["job_id"],
                )
                for file_status in status["files"]:
                    source_surl = file_status["source_surl"]
                    file_path = Url(source_surl).path.strip("/")
                    file_state_parameter = self.get_datafile_state(location=file_path)

                    file_state = file_status["file_state"]
                    state_counter.file_states[file_path] = file_state
                    if file_state_parameter.stringValue != file_state:
                        file_state_parameter.stringValue = file_state
                        self.icat_client.update(bean=file_state_parameter)

            if not state_counter.job_ids:
                beans_to_delete.append(parameter)
            elif state_counter.job_ids != job_ids:
                parameter.stringValue = ",".join(state_counter.job_ids)
                self.icat_client.update(bean=parameter)

            state_parameter = self.get_dataset_state(dataset_id=parameter.dataset.id)
            if state_parameter.stringValue != state_counter.state:
                state_parameter.stringValue = state_counter.state
                self.icat_client.update(bean=state_parameter)

            state_counters.append(state_counter)

        self.icat_client.delete_many(beans=beans_to_delete)

        return state_counters

    def get_update_dataset_status(
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

    def get_dataset_status(
        self,
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
        dataset_parameter = self.get_dataset_state(dataset_id=dataset_id)
        state = dataset_parameter.stringValue
        if list_files:
            datafile_parameters = self.get_datafile_states(dataset_id=dataset_id)
            file_states = {}
            for parameter in datafile_parameters:
                file_states[parameter.datafile.location] = parameter.stringValue
            return DatasetStatusListFilesResponse(state=state, file_states=file_states)
        else:
            return DatasetStatusResponse(state=state)
