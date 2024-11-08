from functools import lru_cache

from fastapi import HTTPException
from pydantic_core import Url
from XRootD import client
from XRootD.client.responses import StatInfo


class XRootDClient:
    """Wrapper for XRootD functionality."""

    def __init__(self, url: str) -> None:
        """Initialise the client for specific XRootD server.

        Args:
            url (str): Unformatted url for connecting to an XRootD server.
        """
        url_object = Url(url)
        self.url_path = url_object.path.replace("//", "/")
        root_url = self._validate_url(url_object)

        self.client = client.FileSystem(url=root_url)

    @staticmethod
    def _validate_url(url_object: Url) -> str:
        """Validates url to ensure it uses the root protocol and port.

        Args:
            url_object (Url): Unvalidated Pydantic Url Object.

        Returns:
            str: Validated url as a str, with the root protocol.
        """
        if url_object.scheme != "root":
            root_url = Url.build(
                scheme="root",
                host=url_object.host,
                port=1094,
            )
        else:
            root_url = Url.build(
                scheme=url_object.scheme,
                host=url_object.host,
                port=url_object.port,
            )

        return str(root_url)

    def stat(self, location: str) -> StatInfo:
        """Stat a file on this XRootD server.

        Args:
            location (str): ICAT Datafile.location for a single object.

        Raises:
            HTTPException: If file is not found in the XRootD server.

        Returns:
            StatInfo: Stat info of the requested file.
        """
        stat_info = self.client.stat(path=f"{self.url_path}{location}")
        status = stat_info[0]
        if status.code != 0:
            raise HTTPException(status_code=status.code, detail=status.message)

        return stat_info[1]


@lru_cache
def get_x_root_d_client(url: str) -> XRootDClient:
    return XRootDClient(url=url)
