class StateCounter:
    """Records state of FTS jobs to determine the overall state to label the Dataset."""

    def __init__(self) -> None:
        """Initialises the counter with all counts at 0."""
        self.job_ids = []
        self.file_states = {}
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
