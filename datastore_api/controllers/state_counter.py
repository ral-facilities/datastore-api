import logging

from datastore_api.models.job import JobState


LOGGER = logging.getLogger(__name__)


class StateCounter:
    """Records state of FTS jobs to determine the overall state to label the Dataset."""

    def __init__(self) -> None:
        """Initialises the counter with all counts at 0."""
        self.job_ids = []
        self.file_states = {}
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

    def check_state(self, state: str, job_id: str) -> None:
        """Counts a single FTS job state, and if non-terminal then records the job_id.

        Args:
            state (str): FTS job state.
            job_id (str): FTS job id.
        """
        self.total += 1
        # Active states
        if state == JobState.staging:
            self.staging += 1
            self.job_ids.append(job_id)
        elif state == JobState.submitted:
            self.submitted += 1
            self.job_ids.append(job_id)
        elif state == JobState.ready:
            self.ready += 1
            self.job_ids.append(job_id)
        elif state == JobState.active:
            self.active += 1
            self.job_ids.append(job_id)
        elif state == JobState.archiving:
            self.archiving += 1
            self.job_ids.append(job_id)
        # Terminal states
        elif state == JobState.canceled:
            self.canceled += 1
        elif state == JobState.failed:
            self.failed += 1
        elif state == JobState.finished_dirty:
            self.finished_dirty += 1
        elif state == JobState.finished:
            self.finished += 1
        else:
            LOGGER.warning("Unexpected FTS job state %s", state)
            self.unknown += 1
