from __future__ import annotations

import os
import pathlib
import typing
from base64 import b64encode

import requests
from flyteidl.service.dataproxy_pb2 import CreateUploadLocationResponse
from fsspec.implementations.http import HTTPFileSystem
from fsspec.callbacks import NoOpCallback
from flytekit.exceptions.user import FlyteAssertion, FlyteValueException
from flytekit.loggers import logger
from flytekit.remote.remote import FlyteRemote
from flytekit.tools.script_mode import hash_file

_DEFAULT_CALLBACK = NoOpCallback()


class RemoteFS(HTTPFileSystem):
    """
    Want this to behave mostly just like the HTTP file system.
    How to do upload of directories? Have to walk and upload each file individually.
    How to download directories? Cannot. Can only retrieve one flyte url literal.

    Error for now, can override later, probably just copy the flyte url parsing stuff for now.
    Can integrate with artifact service in the future.
    def _open()

    Make errors probably
    async def _cat_file(self, url, start=None, end=None, **kwargs):

    Error on flyte urls
    async def _exists(self, path, **kwargs):
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

    # async def _get_file(
    #         self, rpath, lpath, chunk_size=5 * 2 ** 20, callback=_DEFAULT_CALLBACK, **kwargs
    # ):
    #     ...

    def get_upload_link(
        self, local_file_path: str, remote_file_part: str
    ) -> typing.Tuple[CreateUploadLocationResponse, int, bytes]:
        if not pathlib.Path(local_file_path).exists():
            raise AssertionError(f"File {local_file_path} does not exist")

        from flytekit.tools.script_mode import hash_file

        p = pathlib.Path(typing.cast(str, local_file_path))
        md5_bytes, _, content_length = hash_file(p.resolve())
        upload_response = self._remote.client.get_upload_signed_url(
            self._remote.default_project, self._remote.default_domain, md5_bytes, remote_file_part
        )
        logger.debug(f"Resolved signed url {local_file_path} to {upload_response.native_url}")
        print(f"Resolved signed url {local_file_path} to {upload_response.native_url} {upload_response.signed_url}")
        return upload_response, content_length, md5_bytes

    # with open(str(to_upload), "+rb") as local_file:
    #     content = local_file.read()
    #     content_length = len(content)
    #     rsp = requests.put(
    #         upload_location.signed_url,
    #         data=content,
    #         headers={"Content-Length": str(content_length), "Content-MD5": encoded_md5},
    #         verify=False
    #         if self._config.platform.insecure_skip_verify is True
    #         else self._config.platform.ca_cert_file_path,
    #     )

    async def _put_file(
        self,
        lpath,
        rpath,
        chunk_size=5 * 2**20,
        callback=_DEFAULT_CALLBACK,
        method="post",
        **kwargs,
    ):
        """
        fsspec will call this method to upload a file. If recursive, rpath will already be individual files.
        Make the request and upload, but then how do we get the s3 paths back to the user?
        """
        print(f"in _put file: {lpath}, {rpath}")
        # Parse rpath, strip out everything that doesn't make sense.
        rpath = rpath.lstrip("flyte://data/")
        resp, content_length, md5_bytes = self.get_upload_link(lpath, rpath)

        headers = {"Content-Length": str(content_length), "Content-MD5": b64encode(md5_bytes).decode("utf-8")}
        kwargs["headers"] = headers
        rpath = resp.signed_url
        self._local_map[str(pathlib.Path(lpath).absolute())] = resp.native_url
        logger.warning(f"Putting {lpath} to {rpath}")
        await super()._put_file(lpath, rpath, chunk_size, callback=callback, method=method, **kwargs)

    async def _isdir(self, path):
        # should this be always true or always false?
        return True

    # todo: this doesn't work so there's no way to get the s3 link back to set the literal.
    #   and also need to clean up the paths so other_paths works.
    # def put(self, lpath, rpath, recursive=False, callback=None, **kwargs):
    #     print("In remote put override")
    #     return super().put(lpath, rpath, recursive, callback, **kwargs)

    def put_old(self, from_path: str, to_path: str, recursive: bool = False):
        md5_bytes, _ = hash_file(pathlib.Path(from_path).resolve())
        encoded_md5 = b64encode(md5_bytes)
        with open(str(from_path), "+rb") as local_file:
            content = local_file.read()
            content_length = len(content)
            rsp = requests.put(
                to_path,
                data=content,
                headers={"Content-Length": str(content_length), "Content-MD5": encoded_md5},
            )

            if rsp.status_code != requests.codes["OK"]:
                raise FlyteValueException(
                    rsp.status_code,
                    f"Request to send data {to_path} failed.",
                )

    def put_data(self, local_path: typing.Union[str, os.PathLike], remote_path: str, is_multipart: bool = False):
        """
        The implication here is that we're always going to put data to the remote location, so we .remote to ensure
        we don't use the true local proxy if the remote path is a file://

        :param local_path: Local path to the file
        :param remote_path: upload location
        :param is_multipart: Whether to upload Directory or not
        """
        try:
            local_path = str(local_path)
            self.put(typing.cast(str, local_path), remote_path, recursive=is_multipart)
        except Exception as ex:
            raise FlyteAssertion(
                f"Failed to put data from {local_path} to {self._s3_to_signed_url_map[remote_path]} (recursive="
                f"{is_multipart}).\n\n"
                f"Original exception: {str(ex)}"
            ) from ex

    def exists(self, path, **kwargs):
        ...

    def __str__(self):
        p = super().__str__()
        return f"FlyteRemoteFS({self._remote}): {p}"
