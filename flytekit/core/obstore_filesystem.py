"""
Classes that overrides the AsyncFsspecStore that specify the filesystem specific parameters
"""

from typing import Optional

from obstore.fsspec import AsyncFsspecStore

DEFAULT_BLOCK_SIZE = 5 * 2**20


class ObstoreS3FileSystem(AsyncFsspecStore):
    """
    Add following property used in S3FileSystem
    """

    root_marker = ""
    connect_timeout = 5
    retries = 5
    read_timeout = 15
    default_block_size = 5 * 2**20
    protocol = ("s3", "s3a")
    _extra_tokenize_attributes = ("default_block_size",)

    def __init__(self, retries: Optional[int] = None, **kwargs):
        """
        Initialize the ObstoreS3FileSystem with optional retries.

        Args:
            retries (int): Number of retry for requests
            **kwargs: Other keyword arguments passed to the parent class
        """
        if retries is not None:
            self.retries = retries

        super().__init__(**kwargs)


class ObstoreGCSFileSystem(AsyncFsspecStore):
    """
    Add following property used in GCSFileSystem
    """

    scopes = {"read_only", "read_write", "full_control"}
    retries = 6  # number of retries on http failure
    default_block_size = DEFAULT_BLOCK_SIZE
    protocol = "gcs", "gs"
    async_impl = True


class ObstoreAzureBlobFileSystem(AsyncFsspecStore):
    """
    Add following property used in AzureBlobFileSystem
    """

    protocol = "abfs"
