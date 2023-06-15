from __future__ import annotations

import pathlib
import typing
from base64 import b64encode

import fsspec
from flyteidl.service.dataproxy_pb2 import CreateUploadLocationResponse
from fsspec.callbacks import NoOpCallback
from fsspec.implementations.http import HTTPFileSystem
from fsspec.utils import get_protocol

from flytekit.core.utils import write_proto_to_file
from flytekit.loggers import logger
from flytekit.tools.script_mode import hash_file

if typing.TYPE_CHECKING:
    from flytekit.remote.remote import FlyteRemote

_DEFAULT_CALLBACK = NoOpCallback()
_PREFIX_KEY = "upload_prefix"
# This file system is not really a filesystem, so users aren't really able to specify the remote path,
# at least not yet.
REMOTE_PLACEHOLDER = "flyte://data"

"""
todo:
  - check raw output prefix is overrideable from the command line
  - check that put returns strings as expected, and update call locations.
  - when pyflyte run/register runs, make it install a new context with new file access provider
"""


def get_class(r: FlyteRemote) -> typing.Type[RemoteFS]:
    class _RemoteFS(RemoteFS):
        def __init__(self, **storage_options):
            super().__init__(remote=r, **storage_options)

    return _RemoteFS


class RemoteFS(HTTPFileSystem):
    """
    Want this to behave mostly just like the HTTP file system.

    """

    sep = "/"
    protocol = "flyte"

    def __init__(
        self,
        remote: FlyteRemote,
        **storage_options,
    ):
        super().__init__(**storage_options)
        self._remote = remote
        self._local_map: typing.Dict[str, str] = {}

    @property
    def fsid(self) -> str:
        return "flyte"

    async def _get_file(self, rpath, lpath, chunk_size=5 * 2**20, callback=_DEFAULT_CALLBACK, **kwargs):
        """
        Don't do anything special. If it's a flyte url, the create a download link and write to lpath,
        otherwise default to parent.
        """
        if rpath.startswith("flyte://"):
            # naive implementation for now, just write to a file
            resp = self._remote.client.get_data(flyte_uri=rpath)
            write_proto_to_file(resp.literal_map, lpath)
            return lpath

        return await super()._get_file(rpath, lpath, chunk_size=5 * 2**20, callback=_DEFAULT_CALLBACK, **kwargs)

    def get_upload_link(
        self,
        local_file_path: str,
        remote_file_part: str,
        prefix: typing.Optional[str] = None,
    ) -> typing.Tuple[CreateUploadLocationResponse, int, bytes]:
        if not pathlib.Path(local_file_path).exists():
            raise AssertionError(f"File {local_file_path} does not exist")

        p = pathlib.Path(typing.cast(str, local_file_path))
        md5_bytes, _, content_length = hash_file(p.resolve())
        upload_response = self._remote.client.get_upload_signed_url(
            self._remote.default_project,
            self._remote.default_domain,
            md5_bytes,
            remote_file_part,
            filename_root=prefix,
        )
        logger.debug(f"Resolved signed url {local_file_path} to {upload_response.native_url}")
        print(f"Resolved signed url {local_file_path} to {upload_response.native_url} {upload_response.signed_url}")
        return upload_response, content_length, md5_bytes

    async def _put_file(
        self,
        lpath,
        rpath,
        chunk_size=5 * 2**20,
        callback=_DEFAULT_CALLBACK,
        method="put",
        **kwargs,
    ):
        """
        fsspec will call this method to upload a file. If recursive, rpath will already be individual files.
        Make the request and upload, but then how do we get the s3 paths back to the user?
        """
        p = kwargs.pop(_PREFIX_KEY)
        print(f"prefix is {p}")
        # Parse rpath, strip out everything that doesn't make sense.
        rpath = rpath.replace(f"{REMOTE_PLACEHOLDER}/", "", 1)
        resp, content_length, md5_bytes = self.get_upload_link(lpath, rpath, p)

        headers = {"Content-Length": str(content_length), "Content-MD5": b64encode(md5_bytes).decode("utf-8")}
        kwargs["headers"] = headers
        rpath = resp.signed_url
        self._local_map[str(pathlib.Path(lpath).absolute())] = resp.native_url
        logger.debug(f"Writing {lpath} to {rpath}")
        await super()._put_file(lpath, rpath, chunk_size, callback=callback, method=method, **kwargs)
        return resp.native_url

    @staticmethod
    def extract_common(native_urls: typing.List[str]) -> str:
        """
        This function that will take a list of strings and return the longest prefix that they all have in common.
        That is, if you have
            ['s3://my-s3-bucket/flytesnacks/development/ABCYZWMPACZAJ2MABGMOZ6CCPY======/source/empty.md',
             's3://my-s3-bucket/flytesnacks/development/ABCXKL5ZZWXY3PDLM3OONUHHME======/source/nested/more.txt',
             's3://my-s3-bucket/flytesnacks/development/ABCXBAPBKONMADXVW5Q3J6YBWM======/source/original.txt']
        this will return back 's3://my-s3-bucket/flytesnacks/development/'
        Note that trailing characters after a separator that just happen to be the same will also be stripped.
        """
        if len(native_urls) == 0:
            return ""
        if len(native_urls) == 1:
            return native_urls[0]

        common_prefix = ""
        shortest = min([len(x) for x in native_urls])
        x = [[native_urls[j][i] for j in range(len(native_urls))] for i in range(shortest)]
        for i in x:
            if len(set(i)) == 1:
                common_prefix += i[0]
            else:
                break

        fs = fsspec.filesystem(get_protocol(native_urls[0]))
        sep = fs.sep
        # split the common prefix on the last separator so we don't get any trailing characters.
        common_prefix = common_prefix.rsplit(sep, 1)[0]
        logger.debug(f"Returning {common_prefix} from {native_urls}")
        return common_prefix

    async def _put(
        self,
        lpath,
        rpath,
        recursive=False,
        callback=_DEFAULT_CALLBACK,
        batch_size=None,
        **kwargs,
    ):
        """
        cp file.txt flyte://data/some/path/file.txt
        """
        if rpath != REMOTE_PLACEHOLDER:
            logger.debug(f"FlyteRemote FS doesn't yet support specifying full remote path, ignoring {rpath}")

        # maybe we can hash everything here somehow.
        prefix = self._remote.context.file_access.get_random_string()
        # todo: can union:// do better instead of having a placeholder
        kwargs[_PREFIX_KEY] = prefix
        res = await super()._put(lpath, REMOTE_PLACEHOLDER, recursive, callback, batch_size, **kwargs)
        if isinstance(res, list):
            return self.extract_common(res)
        return res

    async def _isdir(self, path):
        # todo should this be always true or always false?
        #  check with single file upload
        return True

    def exists(self, path, **kwargs):
        if str(path).startswith("flyte"):
            raise NotImplementedError("flyte remote currently can't check if a file exists")
        return super().exists(path, **kwargs)

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=None,  # XXX: This differs from the base class.
        cache_type=None,
        cache_options=None,
        size=None,
        **kwargs,
    ):
        # Error for now for flyte, inherit otherwise.
        if str(path).startswith("flyte"):
            raise NotImplementedError("flyte remote currently can't _open yet")
        return super()._open(path, mode, block_size, autocommit, cache_type, cache_options, size, **kwargs)

    async def _cat_file(self, url, start=None, end=None, **kwargs):
        # Just error because it might never cat a file
        raise NotImplementedError("cat file is not implemented for this file system yet.")

    def __str__(self):
        p = super().__str__()
        return f"FlyteRemoteFS({self._remote}): {p}"
