import asyncio
from contextlib import asynccontextmanager
import logging
from typing import AsyncGenerator
from urllib.error import URLError

from codecarbon.emissions_tracker import OfflineEmissionsTracker
from fastapi import FastAPI

from datastore_api.controllers.state_controller import StateController

LOGGER = logging.getLogger(__name__)
CARBON_LOGGER = logging.getLogger("code_carbon")


async def poll_fts_thread() -> None:
    """Starts a thread to poll FTS for the state of archival jobs, and updates ICAT with
    the results.
    """
    state_controller = StateController()
    while True:
        poll_fts(state_controller)
        await asyncio.sleep(60)


async def code_carbon_thread() -> None:
    """Starts a thread to track power usage and estimate CO2 emissions from running the
    API.
    """
    tracker = OfflineEmissionsTracker(country_iso_code="GBR", log_level="warning")
    tracker.start()
    while True:
        tracker.flush()
        await asyncio.sleep(60 * 60)


def poll_fts(state_controller: StateController) -> None:
    """Polls ICAT for FTS job ids that need updating, then poll FTS for the latest
    status and update the ICAT with this information.

    Args:
        icat_client (IcatClient):
            IcatClient to use for queries, with a functional login.
    """
    LOGGER.info("Polling ICAT/FTS for job statuses")
    state_controller.icat_client.client.refresh()
    try:
        parameters = state_controller.get_dataset_job_ids()
        state_controller.update_jobs(parameters)
    except URLError as e:
        LOGGER.error("Unable to poll for job statuses: %s", str(e))


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Creates ongoing task threads.

    Args:
        app (FastAPI): DatastoreAPI instance.

    Returns:
        AsyncGenerator[None, None]
    """
    asyncio.create_task(poll_fts_thread())
    asyncio.create_task(code_carbon_thread())
    yield
