from secrets import token_bytes
from typing import Generator
from uuid import UUID

from botocore.exceptions import ClientError
from fastapi import HTTPException

from datastore_api.clients.fts3_client import get_fts3_client
from datastore_api.clients.s3_client import get_s3_client
from datastore_api.controllers.state_counter import StateCounter
from datastore_api.models.job import COMPLETE_JOB_STATES, TransferState
from datastore_api.models.restore import BucketAcl


class BucketController:
    """Handles logic for restoring to S3 buckets."""

    def __init__(self, name: str = None) -> None:
        """Initialise the controller, either for a new or existing bucket.

        Args:
            name (str, optional):
                If not provided, a random name will be assigned to the bucket.
                Defaults to None.

        Raises:
            HTTPException: If the name of the `cache_bucket` is provided.
        """
        self.fts3_client = get_fts3_client()
        self.s3_client = get_s3_client()
        if name == self.s3_client.cache_bucket:
            raise HTTPException(403, "Access to global S3 cache is forbidden")

        if name is None:
            name = str(UUID(bytes=token_bytes(16), version=4))

        self.bucket = self.s3_client.resource.Bucket(name=name)
        self.job_ids_object = self.s3_client.resource.Object(
            bucket_name=name,
            key=".job_ids",
        )

    @property
    def acl(self) -> BucketAcl:
        """
        Returns:
            BucketAcl: The pre-defined ACL set when the bucket was originally created.
        """
        response = self.s3_client.client.get_bucket_acl(Bucket=self.bucket.name)
        for grant in response["Grants"]:
            all_users_str = "http://acs.amazonaws.com/groups/global/AllUsers"
            try:
                is_group = grant["Grantee"]["Type"] == "Group"
                is_all_users = grant["Grantee"]["URI"] == all_users_str
                is_read = grant["Permission"] == "READ"
                if is_group and is_all_users and is_read:
                    return BucketAcl.PUBLIC_READ
            except KeyError:
                continue

        return BucketAcl.PRIVATE

    @property
    def complete(self) -> bool:
        """
        Returns:
            bool: Whether all the FTS job ids for this bucket are in a terminal state.
        """
        job_ids = []
        job_complete = []
        for job_id, state in self.cached_job_states:
            job_ids.append(job_id)
            job_complete.append(state in COMPLETE_JOB_STATES)

        if all(job_complete):
            return True
        else:
            statuses = self.fts3_client.statuses(job_ids=job_ids, list_files=True)
            state_counter = self.update_job_ids(statuses=statuses, check_files=False)
            return state_counter.state in COMPLETE_JOB_STATES

    @property
    def cached_job_states(self) -> Generator[tuple[str, str], None, None]:
        """
        Yields:
            Generator[tuple[str, str], None, None]:
                Tuple of the FTS job id and last recorded state of that job.
        """
        response = self.job_ids_object.get()
        line = response["Body"].readline()
        while line:
            yield line.decode().split(":")
            line = response["Body"].readline()

    def create(self, bucket_acl: BucketAcl) -> None:
        """Actually create the bucket in the S3 object store.

        Args:
            bucket_acl (BucketAcl): The pre-defined ACL to use for this bucket.
        """
        self.bucket.create(ACL=bucket_acl.value)

    def set_job_ids(self, job_states: dict[str, str]) -> None:
        """Put a mapping of FTS job ids to FTS job states into the reserved .job_ids
        object.

        Args:
            job_states (dict[str, str]): Latest states for the buckets FTS job ids.
        """
        job_ids_string = "\n".join([f"{j[0]}:{j[1]}" for j in job_states.items()])
        self.job_ids_object.put(Body=job_ids_string.encode())

    def update_job_ids(
        self,
        statuses: list[dict[str, str]],
        check_files: bool,
    ) -> StateCounter:
        """Update the .job_ids object with the latest information from FTS. If any jobs
        have fully completed, then the successful files will be copied from the cache to
        this bucket.

        Args:
            statuses (list[dict[str, str]]): Latest status information from FTS.
            check_files (bool): Whether to count the states of individual files.

        Returns:
            StateCounter: Counter for ongoing jobs, overall and individual file states.
        """
        state_counter = StateCounter()
        latest_job_states = {}
        for status in statuses:
            job_id = status["job_id"]
            state = status["job_state"]
            latest_job_states[job_id] = state
            job_complete = state_counter.check_state(state=state, job_id=job_id)
            if check_files or job_complete:
                for file_status in status["files"]:
                    file_path, file_state = state_counter.check_file(file_status)
                    if job_complete and file_state == TransferState.finished:
                        copy_source = {
                            "Bucket": self.s3_client.cache_bucket,
                            "Key": file_path,
                        }
                        self.bucket.copy(CopySource=copy_source, Key=file_path)

        self.set_job_ids(job_states=latest_job_states)
        return state_counter

    def get_data(self, expiration: int) -> dict[str, str]:
        """Get download links for the data in the bucket.

        Args:
            expiration (int):
                Expiration lifetime of the pre-signed url.
                Only used if the bucket ACL is private.

        Raises:
            HTTPException: If restoration still ongoing.

        Returns:
            dict[str, str]:
                Mapping of either the bucket to its url, or each file to a pre-signed
                url depending on the ACL of the bucket.
        """
        if not self.complete:
            raise HTTPException(400, "Restoration of requested data still ongoing")

        if self.acl == BucketAcl.PUBLIC_READ:
            return {"bucket": f"{self.s3_client.endpoint}/{self.bucket.name}"}
        elif self.acl == BucketAcl.PRIVATE:
            links = {}
            for bucket_object in self.bucket.objects.all():
                if bucket_object.key != ".job_ids":
                    links[bucket_object.key] = self.s3_client.create_presigned_url(
                        object_name=bucket_object.key,
                        bucket_name=self.bucket.name,
                        expiration=expiration,
                    )
            return links

    def delete(self) -> None:
        """Cancel all pending FTS jobs, delete all objects, then delete the bucket.

        Raises:
            ClientError: Any S3 ClientError excluding NoSuchBucket, which is excepted.
        """
        try:
            for job_id, state in self.cached_job_states:
                if state not in COMPLETE_JOB_STATES:
                    self.fts3_client.cancel(job_id=job_id)

            self.bucket.objects.delete()
            self.bucket.delete()

        except ClientError as e:
            if "NoSuchBucket" not in str(e):
                raise e
