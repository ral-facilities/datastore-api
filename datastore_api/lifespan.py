import asyncio
from contextlib import asynccontextmanager
import logging
from typing import AsyncGenerator
from urllib.error import URLError

from fastapi import FastAPI
from icat.entities import Entity

from datastore_api.fts3_client import get_fts3_client
from datastore_api.icat_client import IcatClient

LOGGER = logging.getLogger(__name__)


class StateCounter:
    """Records state of FTS jobs to determine the overall state to label the Dataset."""

    def __init__(self) -> None:
        """Initialises the counter with all counts at 0."""
        self.job_ids = []
        self.total = 0
        self.staging = 0
        self.submitted = 0
        self.active = 0
        self.canceled = 0
        self.failed = 0
        self.finished_dirty = 0
        self.finished = 0

    @property
    def state(self) -> str:
        """Determines the appropriate state for an ICAT Dataset.

        If any jobs are in a non-terminal state, then the "earliest" of these in the FTS
        flow is returned. If all jobs are in a single terminal state, then that state is
        used. If all jobs are in different terminal states, then FINISHEDDIRTY is
        returned.

        Returns:
            str: The state applicable to an ICAT Dataset.
        """
        # Active states
        if self.staging:
            return "STAGING"
        elif self.submitted:
            return "SUBMITTED"
        elif self.active:
            return "ACTIVE"
        # Terminal states
        elif self.canceled == self.total:
            return "CANCELED"
        elif self.failed == self.total:
            return "FAILED"
        elif self.finished == self.total:
            return "FINISHED"
        else:
            return "FINISHEDDIRTY"

    def check_state(self, state: str, job_id: str) -> None:
        """Counts a single FTS job state, and if non-terminal then records the job_id.

        Args:
            state (str): FTS job state.
            job_id (str): FTS job id.
        """
        self.total += 1
        # Active states
        if state == "STAGING":
            self.staging += 1
            self.job_ids.append(job_id)
        elif state == "SUBMITTED":
            self.submitted += 1
            self.job_ids.append(job_id)
        elif state == "ACTIVE":
            self.active += 1
            self.job_ids.append(job_id)
        # Terminal states
        elif state == "CANCELED":
            self.canceled += 1
        elif state == "FAILED":
            self.failed += 1
        elif state == "FINISHEDDIRTY":
            self.finished_dirty += 1
        elif state == "FINISHED":
            self.finished += 1


async def poll_fts() -> None:
    """Starts a thread to poll FTS for the state of archival jobs, and updates ICAT with
    the results.
    """
    icat_client = IcatClient()
    icat_client.login_functional()
    while True:
        LOGGER.info("Polling ICAT/FTS for job statuses")
        try:
            parameters = icat_client.get_entities(
                entity="DatasetParameter",
                equals={"type.name": icat_client.settings.parameter_type_job_ids},
                includes="1",
            )
            beans_to_delete = update_jobs(icat_client, parameters)
            icat_client.delete_many(beans=beans_to_delete)
        except URLError as e:
            LOGGER.error("Unable to poll for job statuses: %s", str(e))
        await asyncio.sleep(60)
        icat_client.client.refresh()


def update_jobs(icat_client: IcatClient, parameters: list[Entity]) -> list[Entity]:
    """Updates ICAT Parameter entities with the latest state information from FTS.

    Args:
        icat_client (IcatClient): Client to use for ICAT operations.
        parameters (list[Entity]): DatasetParameter entities containing FTS job ids.

    Returns:
        list[Entity]:
            Entries from `parameters` that can be deleted due to the job entering a
            terminal state.
    """
    beans_to_delete = []
    for parameter in parameters:
        state_counter = StateCounter()
        job_ids = parameter.stringValue.split(",")
        for job_id in job_ids:
            status = get_fts3_client().status(job_id=job_id, list_files=True)
            state_counter.check_state(state=status["job_state"], job_id=job_id)
            for file_status in status["files"]:
                source_surl = file_status["source_surl"]
                file_path = source_surl.split("//")[-1].split("?")[0]
                file_state_parameter = icat_client.get_single_entity(
                    entity="DatafileParameter",
                    equals={
                        "type.name": icat_client.settings.parameter_type_job_state,
                        "datafile.location": file_path,
                    },
                    includes="1",
                )

                file_state = file_status["file_state"]
                if file_state_parameter.stringValue != file_state:
                    file_state_parameter.stringValue = file_state
                    icat_client.update(bean=file_state_parameter)

        if not state_counter.job_ids:
            beans_to_delete.append(parameter)
        elif state_counter.job_ids != job_ids:
            parameter.stringValue = ",".join(state_counter.job_ids)
            icat_client.update(bean=parameter)

        state_parameter = icat_client.get_single_entity(
            entity="DatasetParameter",
            equals={
                "type.name": icat_client.settings.parameter_type_job_state,
                "dataset.id": parameter.dataset.id,
            },
            includes="1",
        )
        if state_parameter.stringValue != state_counter.state:
            state_parameter.stringValue = state_counter.state
            icat_client.update(bean=state_parameter)

    return beans_to_delete


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Creates ongoing task threads.

    Args:
        app (FastAPI): DatastoreAPI instance.

    Returns:
        AsyncGenerator[None, None]
    """
    asyncio.create_task(poll_fts())
    yield
