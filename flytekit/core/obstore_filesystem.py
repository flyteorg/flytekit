"""
Classes that overrides the AsyncFsspecStore that specify the filesystem specific parameters
"""

from obstore.fsspec import AsyncFsspecStore

DEFAULT_BLOCK_SIZE = 5 * 2**20


class ObstoreS3FileSystem(AsyncFsspecStore):
    """
    Add following property used in S3FileSystem
    """

    root_marker = ""
    blocksize = DEFAULT_BLOCK_SIZE
    protocol = ("s3", "s3a")
    _extra_tokenize_attributes = ("default_block_size",)


class ObstoreGCSFileSystem(AsyncFsspecStore):
    """
    Add following property used in GCSFileSystem
    """

    scopes = {"read_only", "read_write", "full_control"}
    blocksize = DEFAULT_BLOCK_SIZE
    protocol = "gcs", "gs"


class ObstoreAzureBlobFileSystem(AsyncFsspecStore):
    """
    Add following property used in AzureBlobFileSystem
    """

    protocol = "abfs"
