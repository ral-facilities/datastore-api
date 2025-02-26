import logging

from pydantic_core import Url

from datastore_api.models.job import COMPLETE_TRANSFER_STATES, JobState


LOGGER = logging.getLogger(__name__)


class StateCounter:
    """Records state of FTS jobs to determine the overall state to label the Dataset."""

    def __init__(self) -> None:
        """Initialises the counter with all counts at 0."""
        self.ongoing_job_ids = []
        self.total = 0
        self.staging = 0
        self.submitted = 0
        self.ready = 0
        self.active = 0
        self.archiving = 0
        self.canceled = 0
        self.failed = 0
        self.finished_dirty = 0
        self.finished = 0
        self.unknown = 0
        self.file_states = {}
        self.files_total = 0
        self.files_complete = 0

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
        if self.unknown:
            return "UNKNOWN"
        # Active states
        elif self.staging:
            return JobState.staging.value
        elif self.submitted:
            return JobState.submitted.value
        elif self.ready:
            return JobState.ready.value
        elif self.active:
            return JobState.active.value
        elif self.archiving:
            return JobState.archiving.value
        # Terminal states
        elif self.canceled == self.total:
            return JobState.canceled.value
        elif self.failed == self.total:
            return JobState.failed.value
        elif self.finished == self.total:
            return JobState.finished.value
        else:
            return JobState.finished_dirty.value

    @property
    def file_percentage(self) -> float:
        """
        Returns:
            float:
                Percentage of file transfers in a terminal state,
                or -1 if no files checked.
        """
        if self.files_total == 0:
            return -1
        else:
            return 100 * self.files_complete / self.files_total

    def check_state(self, state: str, job_id: str = None) -> bool:
        """Counts a single FTS job state, and if non-terminal then records the job_id.

        Args:
            state (str): FTS job state.
            job_id (str): FTS job id.
        """
        self.total += 1
        # Active states
        if state == JobState.staging:
            self.staging += 1
            self.ongoing_job_ids.append(job_id)
        elif state == JobState.submitted:
            self.submitted += 1
            self.ongoing_job_ids.append(job_id)
        elif state == JobState.ready:
            self.ready += 1
            self.ongoing_job_ids.append(job_id)
        elif state == JobState.active:
            self.active += 1
            self.ongoing_job_ids.append(job_id)
        elif state == JobState.archiving:
            self.archiving += 1
            self.ongoing_job_ids.append(job_id)
        # Terminal states
        elif state == JobState.canceled:
            self.canceled += 1
            return True
        elif state == JobState.failed:
            self.failed += 1
            return True
        elif state == JobState.finished_dirty:
            self.finished_dirty += 1
            return True
        elif state == JobState.finished:
            self.finished += 1
            return True
        else:
            LOGGER.warning("Unexpected FTS job state %s", state)
            self.unknown += 1
            return True

        return False

    def check_file(self, file_status: dict[str, str]) -> tuple[str, str]:
        """Parses out the file location and state from the FTS status, and increments
        the relevant file counters.

        Args:
            file_status (dict[str, str]): Latest FTS file status for a single transfer.

        Returns:
            tuple[str, str]: File location (excluding endpoint address), FTS file state.
        """
        file_path, file_state = StateCounter.get_state(file_status=file_status)
        self.file_states[file_path] = file_state
        self.files_total += 1
        if file_state in COMPLETE_TRANSFER_STATES:
            self.files_complete += 1

        return file_path, file_state
    
    def check_datafile_state(self, file_status: str) -> str:
        """Parses out the file location and state from the FTS status, and increments
        the relevant file counters.

        Args:
            file_status (dict[str, str]): Latest FTS file status for a single transfer.

        Returns:
            tuple[str, str]: File location (excluding endpoint address), FTS file state.
        """

        file_state = file_status

        self.files_total += 1
        if file_state in COMPLETE_TRANSFER_STATES:
            self.files_complete += 1

        return file_state

    @staticmethod
    def get_state(file_status: dict[str, str]) -> tuple[str, str]:
        """Parses out the file state from the FTS status.

        Args:
            file_status (dict[str, str]): Latest FTS file status for a single transfer.

        Returns:
            tuple[str, str]: FTS file path, file state.
        """
        source_surl = file_status["source_surl"]
        file_state = file_status["file_state"]
        file_path = Url(source_surl).path.strip("/")

        return file_path, file_state
