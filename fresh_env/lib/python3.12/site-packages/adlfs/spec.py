# -*- coding: utf-8 -*-


from __future__ import absolute_import, division, print_function

import asyncio
import errno
import io
import logging
import os
import re
import typing
import warnings
import weakref
from collections import defaultdict
from datetime import datetime, timedelta
from glob import has_magic
from typing import Optional, Tuple

from azure.core.exceptions import (
    HttpResponseError,
    ResourceExistsError,
    ResourceNotFoundError,
)
from azure.storage.blob import (
    BlobBlock,
    BlobProperties,
    BlobSasPermissions,
    BlobType,
    generate_blob_sas,
)
from azure.storage.blob.aio import BlobPrefix
from azure.storage.blob.aio import BlobServiceClient as AIOBlobServiceClient
from fsspec.asyn import AsyncFileSystem, _get_batch_size, get_loop, sync, sync_wrapper
from fsspec.spec import AbstractBufferedFile
from fsspec.utils import infer_storage_options

from .utils import (
    close_container_client,
    close_credential,
    close_service_client,
    filter_blobs,
    get_blob_metadata,
    match_blob_version,
)

logger = logging.getLogger(__name__)

FORWARDED_BLOB_PROPERTIES = [
    "metadata",
    "creation_time",
    "deleted",
    "deleted_time",
    "last_modified",
    "content_time",
    "content_settings",
    "remaining_retention_days",
    "archive_status",
    "last_accessed_on",
    "etag",
    "tags",
    "tag_count",
]
VERSIONED_BLOB_PROPERTIES = [
    "version_id",
    "is_current_version",
]
_ROOT_PATH = "/"
_DEFAULT_BLOCK_SIZE = 4 * 1024 * 1024

_SOCKET_TIMEOUT_DEFAULT = object()


# https://github.com/Azure/azure-sdk-for-python/issues/11419#issuecomment-628143480
def make_callback(key, callback):
    if callback is None:
        return None

    sent_total = False

    def wrapper(response):
        nonlocal sent_total

        current = response.context.get(key)
        total = response.context["data_stream_total"]
        if current is None:
            return
        if not sent_total and total is not None:
            callback.set_size(total)
        callback.absolute_update(current)

    return wrapper


def get_running_loop():
    # this was removed from fsspec in https://github.com/fsspec/filesystem_spec/pull/1134
    if hasattr(asyncio, "get_running_loop"):
        return asyncio.get_running_loop()
    else:
        loop = asyncio._get_running_loop()
        if loop is None:
            raise RuntimeError("no running event loop")
        else:
            return loop


def _coalesce_version_id(*args) -> Optional[str]:
    """Helper to coalesce a list of version_ids down to one"""
    version_ids = set(args)
    if None in version_ids:
        version_ids.remove(None)
    if len(version_ids) > 1:
        raise ValueError(
            "Cannot coalesce version_ids where more than one are defined,"
            " {}".format(version_ids)
        )
    elif len(version_ids) == 0:
        return None
    else:
        return version_ids.pop()


class AzureBlobFileSystem(AsyncFileSystem):
    """
    Access Azure Datalake Gen2 and Azure Storage if it were a file system using Multiprotocol Access

    Parameters
    ----------
    account_name: str
        The storage account name. This is used to authenticate requests
        signed with an account key and to construct the storage endpoint. It
        is required unless a connection string is given, or if a custom
        domain is used with anonymous authentication.
    account_key: str
        The storage account key. This is used for shared key authentication.
        If any of account key, sas token or client_id is specified, anonymous access
        will be used.
    sas_token: str
        A shared access signature token to use to authenticate requests
        instead of the account key. If account key and sas token are both
        specified, account key will be used to sign. If any of account key, sas token
        or client_id are specified, anonymous access will be used.
    request_session: Session
        The session object to use for http requests.
    connection_string: str
        If specified, this will override all other parameters besides
        request session. See
        http://azure.microsoft.com/en-us/documentation/articles/storage-configure-connection-string/
        for the connection string format.
    credential: azure.core.credentials_async.AsyncTokenCredential or SAS token
        The credentials with which to authenticate.  Optional if the account URL already has a SAS token.
        Can include an instance of TokenCredential class from azure.identity.aio.
    blocksize: int
        The block size to use for download/upload operations. Defaults to hardcoded value of
        ``BlockBlobService.MAX_BLOCK_SIZE``
    client_id: str
        Client ID to use when authenticating using an AD Service Principal client/secret.
    client_secret: str
        Client secret to use when authenticating using an AD Service Principal client/secret.
    tenant_id: str
        Tenant ID to use when authenticating using an AD Service Principal client/secret.
    anon: boolean, optional
        The value to use for whether to attempt anonymous access if no other credential is
        passed. By default (``None``), the ``AZURE_STORAGE_ANON`` environment variable is
        checked. False values (``false``, ``0``, ``f``) will resolve to `False` and
        anonymous access will not be attempted. Otherwise the value for ``anon`` resolves
        to ``True``.
    default_fill_cache: bool = True
        Whether to use cache filling with open by default
    default_cache_type: string ('bytes')
        If given, the default cache_type value used for "open()".  Set to none if no caching
        is desired.  Docs in fsspec
    version_aware : bool (False)
        Whether to support blob versioning.  If enable this will require the
        user to have the necessary permissions for dealing with versioned blobs.
    assume_container_exists: Optional[bool] (None)
        Set this to true to not check for existence of containers at all, assuming they exist.
        None (default) means to warn in case of a failure when checking for existence of a container
        False throws if retrieving container properties fails, which might happen if your
        authentication is only valid at the storage container level, and not the
        storage account level.
    max_concurrency:
        The number of concurrent connections to use when uploading or downloading a blob.
        If None it will be inferred from fsspec.asyn._get_batch_size().
    timeout: int
        Sets the server-side timeout when uploading or downloading a blob.
    connection_timeout: int
        The number of seconds the client will wait to establish a connection to the server
        when uploading or downloading a blob.
    read_timeout: int
        The number of seconds the client will wait, between consecutive read operations,
        for a response from the server while uploading or downloading a blob.

    Pass on to fsspec:

    skip_instance_cache:  to control reuse of instances
    use_listings_cache, listings_expiry_time, max_paths: to control reuse of directory listings

    Examples
    --------

    Authentication with an account_key

    >>> abfs = AzureBlobFileSystem(account_name="XXXX", account_key="XXXX")
    >>> abfs.ls('')

    Authentication with an Azure ServicePrincipal

    >>> abfs = AzureBlobFileSystem(account_name="XXXX", tenant_id=TENANT_ID,
    ...                            client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
    >>> abfs.ls('')

    Authentication with DefaultAzureCredential

    >>> abfs = AzureBlobFileSystem(account_name="XXXX", anon=False)
    >>> abfs.ls('')

    Read files as

    >>> ddf = dd.read_csv('abfs://container_name/folder/*.csv', storage_options={
    ...     'account_name': ACCOUNT_NAME, 'tenant_id': TENANT_ID, 'client_id': CLIENT_ID,
    ...     'client_secret': CLIENT_SECRET})
    ... })

    Sharded Parquet & csv files can be read as:

    >>> ddf = dd.read_csv('abfs://container_name/folder/*.csv', storage_options={
    ...                   'account_name': ACCOUNT_NAME, 'account_key': ACCOUNT_KEY})
    >>> ddf = dd.read_parquet('abfs://container_name/folder.parquet', storage_options={
    ...                       'account_name': ACCOUNT_NAME, 'account_key': ACCOUNT_KEY,})
    """

    protocol = "abfs"

    def __init__(
        self,
        account_name: str = None,
        account_key: str = None,
        connection_string: str = None,
        credential: str = None,
        sas_token: str = None,
        request_session=None,
        socket_timeout=_SOCKET_TIMEOUT_DEFAULT,
        blocksize: int = _DEFAULT_BLOCK_SIZE,
        client_id: str = None,
        client_secret: str = None,
        tenant_id: str = None,
        anon: bool = None,
        location_mode: str = "primary",
        loop=None,
        asynchronous: bool = False,
        default_fill_cache: bool = True,
        default_cache_type: str = "bytes",
        version_aware: bool = False,
        assume_container_exists: Optional[bool] = None,
        max_concurrency: Optional[int] = None,
        timeout: Optional[int] = None,
        connection_timeout: Optional[int] = None,
        read_timeout: Optional[int] = None,
        **kwargs,
    ):
        super_kwargs = {
            k: kwargs.pop(k)
            for k in ["use_listings_cache", "listings_expiry_time", "max_paths"]
            if k in kwargs
        }  # pass on to fsspec superclass
        super().__init__(
            asynchronous=asynchronous, loop=loop or get_loop(), **super_kwargs
        )

        self.account_name = account_name or os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
        self.account_key = account_key or os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
        self.connection_string = connection_string or os.getenv(
            "AZURE_STORAGE_CONNECTION_STRING"
        )
        self.sas_token = sas_token or os.getenv("AZURE_STORAGE_SAS_TOKEN")
        self.client_id = client_id or os.getenv("AZURE_STORAGE_CLIENT_ID")
        self.client_secret = client_secret or os.getenv("AZURE_STORAGE_CLIENT_SECRET")
        self.tenant_id = tenant_id or os.getenv("AZURE_STORAGE_TENANT_ID")
        if anon is not None:
            self.anon = anon
        else:
            self.anon = os.getenv("AZURE_STORAGE_ANON", "true").lower() not in [
                "false",
                "0",
                "f",
            ]
        self.location_mode = location_mode
        self.credential = credential
        self.request_session = request_session
        self.assume_container_exists = assume_container_exists
        if socket_timeout is not _SOCKET_TIMEOUT_DEFAULT:
            warnings.warn(
                "socket_timeout is deprecated and has no effect.", FutureWarning
            )
        self.blocksize = blocksize
        self.default_fill_cache = default_fill_cache
        self.default_cache_type = default_cache_type
        self.version_aware = version_aware

        self._timeout_kwargs = {}
        if timeout is not None:
            self._timeout_kwargs["timeout"] = timeout
        if connection_timeout is not None:
            self._timeout_kwargs["connection_timeout"] = connection_timeout
        if read_timeout is not None:
            self._timeout_kwargs["read_timeout"] = read_timeout

        if (
            self.credential is None
            and self.account_key is None
            and self.sas_token is None
            and self.client_id is not None
        ):
            (
                self.credential,
                self.sync_credential,
            ) = self._get_credential_from_service_principal()
        else:
            self.sync_credential = None

        # Solving issue in https://github.com/fsspec/adlfs/issues/270
        if (
            self.credential is None
            and self.anon is False
            and self.sas_token is None
            and self.account_key is None
        ):
            (
                self.credential,
                self.sync_credential,
            ) = self._get_default_azure_credential(**kwargs)

        self.do_connect()
        weakref.finalize(self, sync, self.loop, close_service_client, self)

        if self.credential is not None:
            weakref.finalize(self, sync, self.loop, close_credential, self)

        if max_concurrency is None:
            batch_size = _get_batch_size()
            if batch_size > 0:
                max_concurrency = batch_size
        self.max_concurrency = max_concurrency

    @classmethod
    def _strip_protocol(cls, path: str):
        """
        Remove the protocol from the input path

        Parameters
        ----------
        path: str
            Path to remove the protocol from

        Returns
        -------
        str
            Returns a path without the protocol
        """
        if isinstance(path, list):
            return [cls._strip_protocol(p) for p in path]

        STORE_SUFFIX = ".dfs.core.windows.net"
        logger.debug(f"_strip_protocol for {path}")
        if not path.startswith(("abfs://", "az://", "abfss://")):
            path = path.lstrip("/")
            path = "abfs://" + path
        ops = infer_storage_options(path)
        if "username" in ops:
            if ops.get("username", None):
                ops["path"] = ops["username"] + ops["path"]
        # we need to make sure that the path retains
        # the format {host}/{path}
        # here host is the container_name
        elif ops.get("host", None):
            if (
                ops["host"].count(STORE_SUFFIX) == 0
            ):  # no store-suffix, so this is container-name
                ops["path"] = ops["host"] + ops["path"]
        url_query = ops.get("url_query")
        if url_query is not None:
            ops["path"] = f"{ops['path']}?{url_query}"

        logger.debug(f"_strip_protocol({path}) = {ops}")
        stripped_path = ops["path"].lstrip("/")
        return stripped_path

    @staticmethod
    def _get_kwargs_from_urls(urlpath):
        """Get the account_name from the urlpath and pass to storage_options"""
        ops = infer_storage_options(urlpath)
        out = {}
        host = ops.get("host", None)
        if host:
            match = re.match(
                r"(?P<account_name>.+)\.(dfs|blob)\.core\.windows\.net", host
            )
            if match:
                account_name = match.groupdict()["account_name"]
                out["account_name"] = account_name
        url_query = ops.get("url_query")
        if url_query is not None:
            from urllib.parse import parse_qs

            parsed = parse_qs(url_query)
            if "versionid" in parsed:
                out["version_aware"] = True
        return out

    def _get_credential_from_service_principal(self):
        """
        Create a Credential for authentication.  This can include a TokenCredential
        client_id, client_secret and tenant_id

        Returns
        -------
        Tuple of (Async Credential, Sync Credential).
        """
        from azure.identity import ClientSecretCredential
        from azure.identity.aio import (
            ClientSecretCredential as AIOClientSecretCredential,
        )

        async_credential = AIOClientSecretCredential(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )

        sync_credential = ClientSecretCredential(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )

        return (async_credential, sync_credential)

    def _get_default_azure_credential(self, **kwargs):
        """
        Create a Credential for authentication using DefaultAzureCredential

        Returns
        -------
        Tuple of (Async Credential, Sync Credential).
        """

        from azure.identity import DefaultAzureCredential
        from azure.identity.aio import (
            DefaultAzureCredential as AIODefaultAzureCredential,
        )

        async_credential = AIODefaultAzureCredential(**kwargs)
        sync_credential = DefaultAzureCredential(**kwargs)

        return (async_credential, sync_credential)

    def do_connect(self):
        """Connect to the BlobServiceClient, using user-specified connection details.
        Tries credentials first, then connection string and finally account key

        Raises
        ------
        ValueError if none of the connection details are available
        """

        try:
            if self.connection_string is not None:
                self.service_client = AIOBlobServiceClient.from_connection_string(
                    conn_str=self.connection_string
                )
            elif self.account_name is not None:
                if hasattr(self, "account_host"):
                    self.account_url: str = f"https://{self.account_host}"
                else:
                    self.account_url: str = (
                        f"https://{self.account_name}.blob.core.windows.net"
                    )

                creds = [self.credential, self.account_key]
                if any(creds):
                    self.service_client = [
                        AIOBlobServiceClient(
                            account_url=self.account_url,
                            credential=cred,
                            _location_mode=self.location_mode,
                        )
                        for cred in creds
                        if cred is not None
                    ][0]
                elif self.sas_token is not None:
                    if not self.sas_token.startswith("?"):
                        self.sas_token = f"?{self.sas_token}"
                    self.service_client = AIOBlobServiceClient(
                        account_url=self.account_url + self.sas_token,
                        credential=None,
                        _location_mode=self.location_mode,
                    )
                else:
                    # Fall back to anonymous login, and assume public container
                    self.service_client = AIOBlobServiceClient(
                        account_url=self.account_url
                    )
            else:
                raise ValueError(
                    "Must provide either a connection_string or account_name with credentials!!"
                )

        except RuntimeError:
            loop = get_loop()
            asyncio.set_event_loop(loop)
            self.do_connect()

        except Exception as e:
            raise ValueError(f"unable to connect to account for {e}")

    def split_path(
        self, path, delimiter="/", return_container: bool = False, **kwargs
    ) -> Tuple[str, str, Optional[str]]:
        """
        Normalize ABFS path string into bucket and key.

        Parameters
        ----------
        path : string
            Input path, like `abfs://my_container/path/to/file`

        delimiter: string
            Delimiter used to split the path

        return_container: bool

        Examples
        --------
        >>> split_path("abfs://my_container/path/to/file")
        ['my_container', 'path/to/file']

        >>> split_path("abfs://my_container/path/to/versioned_file?versionid=some_version_id")
        ['my_container', 'path/to/versioned_file', 'some_version_id']
        """

        if path in ["", delimiter]:
            return "", "", None

        path = self._strip_protocol(path)
        path = path.lstrip(delimiter)
        if "/" not in path:
            # this means path is the container_name
            return path, "", None
        container, keypart = path.split(delimiter, 1)
        key, _, version_id = keypart.partition("?versionid=")
        return (
            container,
            key,
            version_id if self.version_aware and version_id else None,
        )

    def modified(self, path: str) -> datetime:
        return self.info(path)["last_modified"]

    def created(self, path: str) -> datetime:
        return self.info(path)["creation_time"]

    async def _info(self, path, refresh=False, **kwargs):
        """Give details of entry at path
        Returns a single dictionary, with exactly the same information as ``ls``
        would with ``detail=True``.
        The default implementation should calls ls and could be overridden by a
        shortcut. kwargs are passed on to ```ls()``.
        Some file systems might not be able to measure the file's size, in
        which case, the returned dict will include ``'size': None``.
        Returns
        -------
        dict with keys: name (full path in the FS), size (in bytes), type (file,
        directory, or something else) and other FS-specific keys.
        """
        container, path, path_version_id = self.split_path(path)
        fullpath = "/".join([container, path]) if path else container
        version_id = _coalesce_version_id(path_version_id, kwargs.get("version_id"))
        kwargs["version_id"] = version_id

        if fullpath == "":
            return {"name": "", "size": None, "type": "directory"}
        elif path == "":
            if not refresh and _ROOT_PATH in self.dircache:
                out = [o for o in self.dircache[_ROOT_PATH] if o["name"] == container]
                if out:
                    return out[0]
            try:
                async with self.service_client.get_container_client(
                    container=container
                ) as cc:
                    properties = await cc.get_container_properties()
            except ResourceNotFoundError as exc:
                raise FileNotFoundError(
                    errno.ENOENT, "No such container", container
                ) from exc
            info = (await self._details([properties]))[0]
            # Make result consistent with _ls_containers()
            if not info.get("metadata"):
                info["metadata"] = None
            return info

        if not refresh:
            out = self._ls_from_cache(fullpath)
            if out is not None:
                if self.version_aware and version_id is not None:
                    out = [
                        o
                        for o in out
                        if o["name"] == fullpath and match_blob_version(o, version_id)
                    ]
                    if out:
                        return out[0]
                else:
                    out = [o for o in out if o["name"] == fullpath]
                    if out:
                        return out[0]
                    return {"name": fullpath, "size": None, "type": "directory"}

        try:
            async with self.service_client.get_blob_client(container, path) as bc:
                props = await bc.get_blob_properties(version_id=version_id)
            return (await self._details([props]))[0]
        except ResourceNotFoundError:
            pass

        if not version_id:
            if await self._dir_exists(container, path):
                return {"name": fullpath, "size": None, "type": "directory"}

        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), fullpath)

    async def _ls_containers(self, return_glob: bool = False):
        if _ROOT_PATH not in self.dircache or return_glob:
            # This is the case where only the containers are being returned
            logger.info(
                "Returning a list of containers in the azure blob storage account"
            )
            contents = self.service_client.list_containers(include_metadata=True)
            containers = [c async for c in contents]
            files = await self._details(containers)
            self.dircache[_ROOT_PATH] = files

        return self.dircache[_ROOT_PATH]

    async def _ls_blobs(
        self,
        target_path: str,
        container: str,
        path: str,
        delimiter: str = "/",
        return_glob: bool = False,
        version_id: Optional[str] = None,
        versions: bool = False,
        **kwargs,
    ):
        if (version_id or versions) and not self.version_aware:
            raise ValueError(
                "version_id/versions cannot be specified if the filesystem "
                "is not version aware"
            )

        if (
            target_path in self.dircache
            and not return_glob
            and not versions
            and all(
                match_blob_version(b, version_id) for b in self.dircache[target_path]
            )
        ):
            return self.dircache[target_path]

        assert container not in ["", delimiter]
        async with self.service_client.get_container_client(container=container) as cc:
            path = path.strip("/")
            include = ["metadata"]
            if version_id is not None or versions:
                assert self.version_aware
                include.append("versions")
            blobs = cc.walk_blobs(include=include, name_starts_with=path)

        # Check the depth that needs to be screened
        depth = target_path.count("/")
        outblobs = []
        try:
            async for next_blob in blobs:
                if depth in [0, 1] and path == "":
                    outblobs.append(next_blob)
                elif isinstance(next_blob, BlobProperties):
                    if next_blob["name"].count("/") == depth:
                        outblobs.append(next_blob)
                    elif not next_blob["name"].endswith("/") and (
                        next_blob["name"].count("/") == (depth - 1)
                    ):
                        outblobs.append(next_blob)
                else:
                    async for blob_ in next_blob:
                        if isinstance(blob_, BlobProperties) or isinstance(
                            blob_, BlobPrefix
                        ):
                            if blob_["name"].endswith("/"):
                                if blob_["name"].rstrip("/").count("/") == depth:
                                    outblobs.append(blob_)
                                elif blob_["name"].count("/") == depth and (
                                    hasattr(blob_, "size") and blob_["size"] == 0
                                ):
                                    outblobs.append(blob_)
                                else:
                                    pass
                            elif blob_["name"].count("/") == (depth):
                                outblobs.append(blob_)
                            else:
                                pass
        except ResourceNotFoundError:
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), target_path
            )
        finalblobs = await self._details(
            outblobs,
            target_path=target_path,
            return_glob=return_glob,
            version_id=version_id,
            versions=versions,
        )
        if return_glob:
            return finalblobs
        if not finalblobs:
            if not await self._exists(target_path):
                raise FileNotFoundError(
                    errno.ENOENT, os.strerror(errno.ENOENT), target_path
                )
            return []
        if not self.version_aware or finalblobs[0].get("is_current_version"):
            self.dircache[target_path] = finalblobs
        return finalblobs

    async def _ls(
        self,
        path: str,
        detail: bool = False,
        invalidate_cache: bool = False,
        delimiter: str = "/",
        return_glob: bool = False,
        version_id: Optional[str] = None,
        versions: bool = False,
        **kwargs,
    ):
        """
        Create a list of blob names from a blob container

        Parameters
        ----------
        path: str
            Path to an Azure Blob with its container name

        detail: bool
            If False, return a list of blob names, else a list of dictionaries with blob details

        invalidate_cache:  bool
            If True, do not use the cache

        delimiter: str
            Delimiter used to split paths

        version_id: str
            Specific blob version to list

        versions: bool
            If True, list all versions

        return_glob: bool

        """
        logger.debug("abfs.ls() is searching for %s", path)
        target_path = self._strip_protocol(path).strip("/")
        container, path, path_version_id = self.split_path(path)
        version_id = _coalesce_version_id(version_id, path_version_id)

        if invalidate_cache:
            self.dircache.clear()

        if (container in ["", ".", "*", delimiter]) and (path in ["", delimiter]):
            output = await self._ls_containers(return_glob=return_glob)
        else:
            output = await self._ls_blobs(
                target_path,
                container,
                path,
                delimiter=delimiter,
                return_glob=return_glob,
                version_id=version_id,
                versions=versions,
            )

        if versions:
            for entry in output:
                entry_version_id = entry.get("version_id")
                if entry_version_id:
                    entry["name"] += f"?versionid={entry_version_id}"

        if detail:
            return output

        return list(sorted(set([o["name"] for o in output])))

    async def _details(
        self,
        contents,
        delimiter="/",
        return_glob: bool = False,
        target_path="",
        version_id: Optional[str] = None,
        versions: bool = False,
        **kwargs,
    ):
        """
        Return a list of dictionaries of specifying details about the contents

        Parameters
        ----------
        contents

        delimiter: str
            Delimiter used to separate containers and files

        return_glob: bool

        version_id: str
            Specific target version to be returned

        versions: bool
            If True, return all versions

        Returns
        -------
        List of dicts
            Returns details about the contents, such as name, size and type
        """
        output = []
        for content in contents:
            data = {
                key: content[key]
                for key in FORWARDED_BLOB_PROPERTIES
                if content.has_key(key)  # NOQA
            }
            if self.version_aware:
                data.update(
                    (key, content[key])
                    for key in VERSIONED_BLOB_PROPERTIES
                    if content.has_key(key)  # NOQA
                )
            if content.has_key("container"):  # NOQA
                fname = f"{content.container}{delimiter}{content.name}"
                fname = fname.rstrip(delimiter)
                if content.has_key("size"):  # NOQA
                    data["name"] = fname
                    data["size"] = content.size
                    data["type"] = "file"
                else:
                    data["name"] = fname
                    data["size"] = None
                    data["type"] = "directory"
            else:
                fname = f"{content.name}"
                data["name"] = fname
                data["size"] = None
                data["type"] = "directory"
            if data.get("metadata"):
                if data["metadata"].get("is_directory") == "true":
                    data["type"] = "directory"
                    data["size"] = None
                elif data["metadata"].get("is_directory") == "false":
                    data["type"] = "file"
                elif (
                    # In some cases Hdi_isfolder is capitalized, see #440
                    data["metadata"].get("hdi_isfolder") == "true"
                    or data["metadata"].get("Hdi_isfolder") == "true"
                ):
                    data["type"] = "directory"
                    data["size"] = None
            if return_glob:
                data["name"] = data["name"].rstrip("/")
            output.append(data)
        if target_path:
            if (
                len(output) == 1
                and output[0]["type"] == "file"
                and not self.version_aware
            ):
                # This handles the case where path is a file passed to ls
                return output
            output = await filter_blobs(
                output,
                target_path,
                delimiter,
                version_id=version_id,
                versions=versions,
            )

        return output

    async def _find(self, path, withdirs=False, prefix="", **kwargs):
        """List all files below path.
        Like posix ``find`` command without conditions.

        Parameters
        ----------
        path : str
            The path (directory) to list from
        withdirs: bool
            Whether to include directory paths in the output. This is True
            when used by glob, but users usually only want files.
        prefix: str
            Only return files that match `^{path}/{prefix}`
        kwargs are passed to ``ls``.
        """
        full_path = self._strip_protocol(path)
        full_path = full_path.strip("/")
        if await self._isfile(full_path):
            return [full_path]
        if prefix != "":
            prefix = prefix.strip("/")
            target_path = f"{full_path}/{prefix}"
        else:
            target_path = f"{full_path}/"

        container, path, _ = self.split_path(target_path)

        async with self.service_client.get_container_client(
            container=container
        ) as container_client:
            blobs = container_client.list_blobs(
                include=["metadata"], name_starts_with=path
            )
        files = {}
        dir_set = set()
        dirs = {}
        detail = kwargs.pop("detail", False)
        try:
            infos = await self._details([b async for b in blobs])
        except ResourceNotFoundError:
            # find doesn't raise but returns [] or {} instead
            infos = []

        for info in infos:
            name = _name = info["name"]
            while True:
                parent_dir = self._parent(_name).rstrip("/")
                if parent_dir not in dir_set and parent_dir != full_path.strip("/"):
                    dir_set.add(parent_dir)
                    dirs[parent_dir] = {
                        "name": parent_dir,
                        "type": "directory",
                        "size": 0,
                    }
                    _name = parent_dir.rstrip("/")
                else:
                    break

            if info["type"] == "directory":
                dirs[name] = info
            if info["type"] == "file":
                files[name] = info

        if not infos:
            try:
                file = await self._info(full_path)
            except FileNotFoundError:
                pass
            else:
                files[file["name"]] = file

        if withdirs:
            files.update(dirs)
        files = {k: v for k, v in files.items() if k.startswith(target_path)}
        names = sorted([n for n in files.keys()])
        if not detail:
            return names
        return {name: files[name] for name in names}

    def _walk(self, path, dirs, files):
        for p, d, f in zip([path], [dirs], [files]):
            yield p, d, f

    async def _async_walk(self, path: str, maxdepth=None, **kwargs):
        """Return all files belows path

        List all files, recursing into subdirectories; output is iterator-style,
        like ``os.walk()``. For a simple list of files, ``find()`` is available.

        Note that the "files" outputted will include anything that is not
        a directory, such as links.

        Parameters
        ----------
        path: str
            Root to recurse into

        maxdepth: int
            Maximum recursion depth. None means limitless, but not recommended
            on link-based file-systems.

        **kwargs are passed to ``ls``
        """
        path = self._strip_protocol(path)
        full_dirs = {}
        dirs = {}
        files = {}

        detail = kwargs.pop("detail", False)
        try:
            listing = await self._ls(path, detail=True, return_glob=True, **kwargs)
        except (FileNotFoundError, IOError):
            listing = []

        for info in listing:
            # each info name must be at least [path]/part , but here
            # we check also for names like [path]/part/
            pathname = info["name"].rstrip("/")
            name = pathname.rsplit("/", 1)[-1]
            if info["type"] == "directory" and pathname != path:
                # do not include "self" path
                full_dirs[pathname] = info
                dirs[name] = info
            elif pathname == path:
                # file-like with same name as give path
                files[""] = info
            else:
                files[name] = info

        if detail:
            for p, d, f in self._walk(path, dirs, files):
                yield p, d, f
        else:
            yield path, list(dirs), list(files)

        if maxdepth is not None:
            maxdepth -= 1
            if maxdepth < 1:
                return

        for d in full_dirs:
            async for path, dirs, files in self._async_walk(
                d, maxdepth=maxdepth, detail=detail, **kwargs
            ):
                yield path, dirs, files

    async def _container_exists(self, container_name):
        if self.assume_container_exists:
            return True
        try:
            async with self.service_client.get_container_client(
                container_name
            ) as client:
                await client.get_container_properties()
        except ResourceNotFoundError:
            return False
        except Exception as e:
            if self.assume_container_exists is None:
                warnings.warn(
                    f"Failed to fetch container properties for {container_name}. Assume it exists already",
                )
                return True
            else:
                raise ValueError(
                    f"Failed to fetch container properties for {container_name} for {e}"
                ) from e
        else:
            return True

    async def _mkdir(self, path, create_parents=True, delimiter="/", **kwargs):
        """
        Mkdir is a no-op for creating anything except top-level containers.
        This aligns to the Azure Blob Filesystem flat hierarchy

        Parameters
        ----------
        path: str
            The path to create

        create_parents: bool
            If True (default), create the Azure Container if it does not exist

        delimiter: str
            Delimiter to use when splitting the path

        """
        fullpath = path
        container_name, path, _ = self.split_path(path, delimiter=delimiter)
        container_exists = await self._container_exists(container_name)
        if not create_parents and not container_exists:
            raise PermissionError(
                "Azure Container does not exist.  Set create_parents=True to create!!"
            )

        if container_exists and not kwargs.get("exist_ok", True):
            raise FileExistsError(
                f"Cannot overwrite existing Azure container -- {container_name} already exists."
            )

        if not container_exists:
            try:
                await self.service_client.create_container(container_name)
                self.invalidate_cache(_ROOT_PATH)

            except Exception as e:
                raise ValueError(
                    f"Proposed container_name of {container_name} does not meet Azure requirements with error {e}!"
                ) from e

        self.invalidate_cache(self._parent(fullpath))

    mkdir = sync_wrapper(_mkdir)

    def makedir(self, path, exist_ok=False):
        """
        Create directory entry at path

        Parameters
        ----------
        path: str
            The path to create

        delimiter: str
            Delimiter to use when splitting the path

        exist_ok: bool
            If False (default), raise an error if the directory already exists.
        """
        try:
            self.mkdir(path, create_parents=True, exist_ok=exist_ok)
        except FileExistsError:
            if exist_ok:
                pass
            else:
                raise

    async def _rm(
        self,
        path: typing.Union[str, typing.List[str]],
        recursive: bool = False,
        maxdepth: typing.Optional[int] = None,
        delimiter: str = "/",
        expand_path: bool = True,
        **kwargs,
    ):
        """Delete files.

        Parameters
        ----------
        path: str or list of str
            File(s) to delete.
        recursive: bool
            Defaults to False.
            If file(s) are directories, recursively delete contents and then
            also remove the directory.
            Only used if `expand_path`.

        maxdepth: int or None
            Defaults to None.
            Depth to pass to walk for finding files to delete, if recursive.
            If None, there will be no limit and infinite recursion may be
            possible.
            Only used if `expand_path`.
        expand_path: bool
            Defaults to True.
            If False, `self._expand_path` call will be skipped. This is more
            efficient when you don't need the operation.
        """
        if expand_path:
            path = await self._expand_path(
                path,
                recursive=recursive,
                maxdepth=maxdepth,
            )
        elif isinstance(path, str):
            path = [path]

        grouped_files = defaultdict(list)
        try:
            for p in reversed(path):
                container_name, p, _ = self.split_path(p, delimiter=delimiter)
                if p != "":
                    grouped_files[container_name].append(p.rstrip(delimiter))
                else:
                    await self._rmdir(container_name)

            for container_name, files in grouped_files.items():
                await self._rm_files(container_name, files)
        except ResourceNotFoundError:
            pass
        except FileNotFoundError:
            pass
        except Exception as e:
            raise RuntimeError("Failed to remove %s for %s", path, e)

        self.invalidate_cache()

    rm = sync_wrapper(_rm)

    async def _rm_files(
        self, container_name: str, file_paths: typing.Iterable[str], **kwargs
    ):
        """
        Delete the given file(s)

        Parameters
        ----------
        file_paths: iterable of str
            File(s) to delete.
        """
        async with self.service_client.get_container_client(
            container=container_name
        ) as cc:
            (
                files,
                directory_markers,
            ) = await self._separate_directory_markers_for_non_empty_directories(
                file_paths
            )

            # Files and directory markers of empty directories can be deleted in any order. We delete them all
            # asynchronously for performance reasons.
            file_exs = await asyncio.gather(
                *([cc.delete_blob(file) for file in files]), return_exceptions=True
            )
            for ex in file_exs:
                if ex is not None:
                    raise ex

            # Directory markers of non-empty directories must be deleted in reverse order to avoid deleting a directory
            # marker before the directory is empty. If these are deleted out of order we will get
            # `This operation is not permitted on a non-empty directory.` on hierarchical namespace storage accounts.
            for directory_marker in reversed(directory_markers):
                await cc.delete_blob(directory_marker)

        for file in file_paths:
            self.invalidate_cache(self._parent(file))

    sync_wrapper(_rm_files)

    async def _separate_directory_markers_for_non_empty_directories(
        self, file_paths: typing.Iterable[str]
    ) -> typing.Tuple[typing.List[str], typing.List[str]]:
        """
        Distinguish directory markers of non-empty directories from files and directory markers for empty directories.
        A directory marker is an empty blob who's name is the path of the directory.
        """
        unique_sorted_file_paths = sorted(set(file_paths))  # Remove duplicates and sort
        directory_markers = []
        files = [
            unique_sorted_file_paths[-1]
        ]  # The last file lexographically cannot be a directory marker for a non-empty directory.

        for file, next_file in zip(
            unique_sorted_file_paths, unique_sorted_file_paths[1:]
        ):
            # /path/to/directory -- directory marker
            # /path/to/directory/file  -- file in directory
            # /path/to/directory2/file -- file in different directory
            if next_file.startswith(file + "/"):
                directory_markers.append(file)
            else:
                files.append(file)

        return files, directory_markers

    def rmdir(self, path: str, delimiter="/", **kwargs):
        sync(self.loop, self._rmdir, path, delimiter=delimiter, **kwargs)

    async def _rmdir(self, path: str, delimiter="/", **kwargs):
        """
        Remove a directory, if empty

        Parameters
        ----------
        path: str
            Path of directory to remove

        delimiter: str
            Delimiter to use when splitting the path

        """

        container_name, path, _ = self.split_path(path, delimiter=delimiter)
        container_exists = await self._container_exists(container_name)
        if container_exists and not path:
            await self.service_client.delete_container(container_name)
            self.invalidate_cache(_ROOT_PATH)

    def size(self, path):
        return sync(self.loop, self._size, path)

    async def _size(self, path):
        """Size in bytes of file"""
        res = await self._info(path)
        size = res.get("size", None)
        return size

    def isfile(self, path):
        return sync(self.loop, self._isfile, path)

    async def _isfile(self, path):
        """Is this entry file-like?"""
        try:
            path_ = path.split("/")[:-1]
            path_ = "/".join([p for p in path_])
            if self.dircache[path_]:
                for fp in self.dircache[path_]:
                    if fp["name"] == path and fp["type"] == "file":
                        return True
        except KeyError:
            pass
        except FileNotFoundError:
            pass
        try:
            container_name, path, version_id = self.split_path(path)
            if not path:
                # A container can not be a file
                return False
            else:
                try:
                    async with self.service_client.get_blob_client(
                        container_name, path
                    ) as bc:
                        props = await bc.get_blob_properties(version_id=version_id)
                    if props["metadata"]["is_directory"] == "false":
                        return True

                except ResourceNotFoundError:
                    return False

                except HttpResponseError:
                    if version_id is not None:
                        return False
                    raise

                except KeyError:
                    details = await self._details([props])
                    return details[0]["type"] == "file"
        except:  # noqa: E722
            return False

    def isdir(self, path):
        return sync(self.loop, self._isdir, path)

    async def _isdir(self, path):
        """Is this entry directory-like?"""
        if path in self.dircache:
            for fp in self.dircache[path]:
                # Files will contain themselves in the cache, but
                # a directory can not contain itself
                if fp["name"] != path:
                    return True
        try:
            container_name, path_, _ = self.split_path(path)
            if not path_:
                return await self._container_exists(container_name)
            else:
                if await self._exists(path) and not await self._isfile(path):
                    return True
                else:
                    return False
        except IOError:
            return False

    def exists(self, path):
        return sync(self.loop, self._exists, path)

    async def _exists(self, path):
        """Is there a file at the given path"""
        try:
            if self._ls_from_cache(path):
                return True
        except FileNotFoundError:
            pass
        except KeyError:
            pass

        container_name, path, version_id = self.split_path(path)

        if not path:
            if container_name:
                return await self._container_exists(container_name)
            else:
                # Empty paths exist by definition
                return True

        async with self.service_client.get_blob_client(container_name, path) as bc:
            try:
                if await bc.exists(version_id=version_id):
                    return True
            except HttpResponseError:
                if version_id is not None:
                    return False
                raise
        return await self._dir_exists(container_name, path)

    async def _dir_exists(self, container, path):
        dir_path = path.rstrip("/") + "/"
        try:
            async with self.service_client.get_container_client(
                container=container
            ) as container_client:
                async for blob in container_client.list_blobs(
                    results_per_page=1, name_starts_with=dir_path
                ):
                    return True
                else:
                    return False
        except ResourceNotFoundError:
            return False

    async def _pipe_file(
        self, path, value, overwrite=True, max_concurrency=None, **kwargs
    ):
        """Set the bytes of given file"""
        container_name, path, _ = self.split_path(path)
        async with self.service_client.get_blob_client(
            container=container_name, blob=path
        ) as bc:
            result = await bc.upload_blob(
                data=value,
                overwrite=overwrite,
                metadata={"is_directory": "false"},
                max_concurrency=max_concurrency or self.max_concurrency,
                **self._timeout_kwargs,
                **kwargs,
            )
        self.invalidate_cache(self._parent(path))
        return result

    pipe_file = sync_wrapper(_pipe_file)

    async def _pipe(self, *args, batch_size=None, max_concurrency=None, **kwargs):
        max_concurrency = max_concurrency or 1
        return await super()._pipe(
            *args, batch_size=batch_size, max_concurrency=max_concurrency, **kwargs
        )

    async def _cat_file(
        self, path, start=None, end=None, max_concurrency=None, **kwargs
    ):
        stripped_path = self._strip_protocol(path)
        if end is not None:
            start = start or 0  # download_blob requires start if length is provided.
            length = end - start
        else:
            length = None
        container_name, blob, version_id = self.split_path(stripped_path)
        async with self.service_client.get_blob_client(
            container=container_name, blob=blob
        ) as bc:
            try:
                stream = await bc.download_blob(
                    offset=start,
                    length=length,
                    version_id=version_id,
                    max_concurrency=max_concurrency or self.max_concurrency,
                    **self._timeout_kwargs,
                )
            except ResourceNotFoundError as e:
                raise FileNotFoundError(
                    errno.ENOENT, os.strerror(errno.ENOENT), path
                ) from e
            except HttpResponseError as e:
                if version_id is not None:
                    raise FileNotFoundError(
                        errno.ENOENT, os.strerror(errno.ENOENT), path
                    ) from e
                raise
            result = await stream.readall()
            return result

    def cat(self, path, recursive=False, on_error="raise", **kwargs):
        """Fetch (potentially multiple) paths' contents
        Returns a dict of {path: contents} if there are multiple paths
        or the path has been otherwise expanded
        on_error : "raise", "omit", "return"
            If raise, an underlying exception will be raised (converted to KeyError
            if the type is in self.missing_exceptions); if omit, keys with exception
            will simply not be included in the output; if "return", all keys are
            included in the output, but the value will be bytes or an exception
            instance.
        """
        paths = self.expand_path(path, recursive=recursive, skip_noexist=False)
        if (
            len(paths) > 1
            or isinstance(path, list)
            or paths[0] != self._strip_protocol(path)
        ):
            out = {}
            for path in paths:
                try:
                    out[path] = self.cat_file(path, **kwargs)
                except Exception as e:
                    if on_error == "raise":
                        raise
                    if on_error == "return":
                        out[path] = e
            return out
        else:
            return self.cat_file(paths[0])

    async def _cat_ranges(self, *args, batch_size=None, max_concurrency=None, **kwargs):
        max_concurrency = max_concurrency or 1
        return await super()._cat_ranges(
            *args, batch_size=batch_size, max_concurrency=max_concurrency, **kwargs
        )

    def url(self, path, expires=3600, **kwargs):
        return sync(self.loop, self._url, path, expires, **kwargs)

    async def _url(
        self,
        path,
        expires=3600,
        content_disposition=None,
        content_encoding=None,
        content_language=None,
        content_type=None,
        **kwargs,
    ):
        """Generate presigned URL to access path by HTTP

        Parameters
        ----------
        path : string
            the key path we are interested in
        expires : int
            the number of seconds this signature will be good for.
        content_disposition : string
            Response header value for Content-Disposition when this URL is accessed.
        content_encoding: string
            Response header value for Content-Encoding when this URL is accessed.
        content_language: string
            Response header value for Content-Language when this URL is accessed.
        content_type: string
            Response header value for Content-Type when this URL is accessed.
        """
        container_name, blob, version_id = self.split_path(path)

        sas_token = generate_blob_sas(
            account_name=self.account_name,
            container_name=container_name,
            blob_name=blob,
            account_key=self.account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + timedelta(seconds=expires),
            version_id=version_id,
            content_disposition=content_disposition,
            content_encoding=content_encoding,
            content_language=content_language,
            content_type=content_type,
        )

        async with self.service_client.get_blob_client(container_name, blob) as bc:
            url = f"{bc.url}?{sas_token}"
        return url

    def expand_path(self, path, recursive=False, maxdepth=None, skip_noexist=True):
        return sync(
            self.loop, self._expand_path, path, recursive, maxdepth, skip_noexist
        )

    async def _expand_path(
        self, path, recursive=False, maxdepth=None, skip_noexist=True, **kwargs
    ):
        """Turn one or more globs or directories into a list of all matching files"""
        if isinstance(path, list):
            path = [f"{p.strip('/')}" for p in path if not p.endswith("*")]
        else:
            if not path.endswith("*"):
                path = f"{path.strip('/')}"
        if isinstance(path, str):
            out = await self._expand_path(
                [path],
                recursive,
                maxdepth,
            )
        else:
            out = set()
            split_paths = [self.split_path(p) for p in path]
            for container, p, version_id in split_paths:
                glob_p = "/".join([container, p]) if p else container
                fullpath = (
                    f"{glob_p}?versionid={version_id}"
                    if version_id is not None
                    else glob_p
                )
                if has_magic(glob_p):
                    bit = set(await self._glob(glob_p))
                    out |= bit
                    if recursive:
                        bit2 = set(await self._expand_path(glob_p))
                        out |= bit2
                    continue
                elif recursive:
                    rec = set(
                        await self._find(
                            glob_p,
                            withdirs=True,
                            version_id=version_id,
                        )
                    )
                    out |= rec

                if fullpath not in out and (
                    recursive is False
                    or await self._exists(fullpath)
                    or await self._exists(fullpath.rstrip("/"))
                ):
                    if skip_noexist and not await self._exists(fullpath):
                        # This is to verify that we don't miss files
                        fullpath = fullpath.rstrip("/")
                        if not await self._exists(fullpath):
                            continue
                    out.add(fullpath)

        if not out:
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path)
        return list(sorted(out))

    async def _put_file(
        self,
        lpath,
        rpath,
        delimiter="/",
        overwrite=True,
        callback=None,
        max_concurrency=None,
        **kwargs,
    ):
        """
        Copy single file to remote

        :param lpath: Path to local file
        :param rpath: Path to remote file
        :param delimitier: Filepath delimiter
        :param overwrite: Boolean (True). Whether to overwrite any existing file
            (True) or raise if one already exists (False).
        """

        container_name, path, _ = self.split_path(rpath, delimiter=delimiter)

        if os.path.isdir(lpath):
            await self._mkdir(rpath)
        else:
            try:
                with open(lpath, "rb") as f1:
                    async with self.service_client.get_blob_client(
                        container_name, path
                    ) as bc:
                        await bc.upload_blob(
                            f1,
                            overwrite=overwrite,
                            metadata={"is_directory": "false"},
                            raw_response_hook=make_callback(
                                "upload_stream_current", callback
                            ),
                            max_concurrency=max_concurrency or self.max_concurrency,
                            **self._timeout_kwargs,
                        )
                self.invalidate_cache()
            except ResourceExistsError:
                raise FileExistsError("File already exists!")
            except ResourceNotFoundError:
                if not await self._exists(container_name):
                    raise FileNotFoundError(
                        errno.ENOENT, "No such container", container_name
                    )
                await self._put_file(lpath, rpath, delimiter, overwrite)
                self.invalidate_cache()

    put_file = sync_wrapper(_put_file)

    async def _put(self, *args, batch_size=None, max_concurrency=None, **kwargs):
        max_concurrency = max_concurrency or 1
        return await super()._put(
            *args, batch_size=batch_size, max_concurrency=max_concurrency, **kwargs
        )

    async def _cp_file(self, path1, path2, **kwargs):
        """Copy the file at path1 to path2"""
        container1, blob1, version_id = self.split_path(path1, delimiter="/")
        container2, blob2, _ = self.split_path(path2, delimiter="/")

        cc1 = self.service_client.get_container_client(container1)
        blobclient1 = cc1.get_blob_client(blob=blob1)
        if container1 == container2:
            blobclient2 = cc1.get_blob_client(blob=blob2)
        else:
            cc2 = self.service_client.get_container_client(container2)
            blobclient2 = cc2.get_blob_client(blob=blob2)
        url = (
            blobclient1.url
            if version_id is None
            else f"{blobclient1.url}?versionid={version_id}"
        )
        try:
            await blobclient2.start_copy_from_url(url)
        except ResourceNotFoundError as e:
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), path1
            ) from e
        self.invalidate_cache(container1)
        self.invalidate_cache(container2)

    cp_file = sync_wrapper(_cp_file)

    def upload(self, lpath, rpath, recursive=False, **kwargs):
        """Alias of :ref:`FilesystemSpec.put`."""
        return self.put(lpath, rpath, recursive=recursive, **kwargs)

    def download(self, rpath, lpath, recursive=False, **kwargs):
        """Alias of :ref:`FilesystemSpec.get`."""
        return self.get(rpath, lpath, recursive=recursive, **kwargs)

    def sign(self, path, expiration=100, **kwargs):
        """Create a signed URL representing the given path."""
        return self.url(path, expires=expiration, **kwargs)

    async def _get_file(
        self,
        rpath,
        lpath,
        recursive=False,
        delimiter="/",
        callback=None,
        max_concurrency=None,
        **kwargs,
    ):
        """Copy single file remote to local"""
        if os.path.isdir(lpath):
            return
        container_name, path, version_id = self.split_path(rpath, delimiter=delimiter)
        try:
            async with self.service_client.get_blob_client(
                container_name, path.rstrip(delimiter)
            ) as bc:
                stream = await bc.download_blob(
                    raw_response_hook=make_callback(
                        "download_stream_current", callback
                    ),
                    version_id=version_id,
                    max_concurrency=max_concurrency or self.max_concurrency,
                    **self._timeout_kwargs,
                )
                with open(lpath, "wb") as my_blob:
                    await stream.readinto(my_blob)
        except ResourceNotFoundError as exception:
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), rpath
            ) from exception

    get_file = sync_wrapper(_get_file)

    async def _get(self, *args, batch_size=None, max_concurrency=None, **kwargs):
        max_concurrency = max_concurrency or 1
        return await super()._get(
            *args, batch_size=batch_size, max_concurrency=max_concurrency, **kwargs
        )

    def getxattr(self, path, attr):
        meta = self.info(path).get("metadata", {})
        return meta[attr]

    async def _setxattrs(self, rpath, **kwargs):
        container_name, path, _ = self.split_path(rpath)
        try:
            async with self.service_client.get_blob_client(container_name, path) as bc:
                await bc.set_blob_metadata(metadata=kwargs)
            self.invalidate_cache(self._parent(rpath))
        except Exception as e:
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), rpath
            ) from e

    setxattrs = sync_wrapper(_setxattrs)

    def invalidate_cache(self, path=None):
        if path is None:
            self.dircache.clear()
        else:
            self.dircache.pop(path, None)
        super(AzureBlobFileSystem, self).invalidate_cache(path)

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int = None,
        autocommit: bool = True,
        cache_options: dict = {},
        cache_type="readahead",
        metadata=None,
        version_id: Optional[str] = None,
        **kwargs,
    ):
        """Open a file on the datalake, or a block blob

        Parameters
        ----------
        path: str
            Path to file to open

        mode: str
            What mode to open the file in - defaults to "rb"

        block_size: int
            Size per block for multi-part downloads.

        autocommit: bool
            Whether or not to write to the destination directly

        cache_type: str
            One of "readahead", "none", "mmap", "bytes", defaults to "readahead"
            Caching policy in read mode.
            See the definitions here:
            https://filesystem-spec.readthedocs.io/en/latest/api.html#readbuffering

        version_id: str
            Explicit version of the blob to open.  This requires that the abfs filesystem
            is versioning aware and blob versioning is enabled on the releveant container.
        """
        logger.debug(f"_open:  {path}")
        if not self.version_aware and version_id:
            raise ValueError(
                "version_id cannot be specified if the filesystem "
                "is not version aware"
            )
        return AzureBlobFile(
            fs=self,
            path=path,
            mode=mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_options=cache_options,
            cache_type=cache_type,
            metadata=metadata,
            version_id=version_id,
            **kwargs,
        )


class AzureBlobFile(AbstractBufferedFile):
    """File-like operations on Azure Blobs"""

    DEFAULT_BLOCK_SIZE = 5 * 2**20

    def __init__(
        self,
        fs: AzureBlobFileSystem,
        path: str,
        mode: str = "rb",
        block_size="default",
        autocommit: bool = True,
        cache_type: str = "bytes",
        cache_options: dict = {},
        metadata=None,
        version_id: Optional[str] = None,
        **kwargs,
    ):
        """
        Represents a file on AzureBlobStorage that implements buffered reading and writing

        Parameters
        ----------
        fs: AzureBlobFileSystem
            An instance of the filesystem

        path: str
            The location of the file on the filesystem

        mode: str
            What mode to open the file in. Defaults to "rb"

        block_size: int, str
            Buffer size for reading and writing. The string "default" will use the class
            default

        autocommit: bool
            Whether or not to write to the destination directly

        cache_type: str
            One of "readahead", "none", "mmap", "bytes", defaults to "readahead"
            Caching policy in read mode. See the definitions in ``core``.

        cache_options : dict
            Additional options passed to the constructor for the cache specified
            by `cache_type`.

        version_id : str
            Optional version to read the file at.  If not specified this will
            default to the current version of the object.  This is only used for
            reading.

        kwargs: dict
            Passed to AbstractBufferedFile
        """

        from fsspec.core import caches

        container_name, blob, path_version_id = fs.split_path(path)
        self.fs = fs
        self.path = path
        self.mode = mode
        self.container_name = container_name
        self.blob = blob
        self.block_size = block_size
        self.version_id = (
            _coalesce_version_id(version_id, path_version_id)
            if self.fs.version_aware
            else None
        )

        try:
            # Need to confirm there is an event loop running in
            # the thread. If not, create the fsspec loop
            # and set it.  This is to handle issues with
            # Async Credentials from the Azure SDK
            loop = get_running_loop()

        except RuntimeError:
            loop = get_loop()
            asyncio.set_event_loop(loop)

        self.loop = self.fs.loop or get_loop()
        self.container_client = (
            fs.service_client.get_container_client(self.container_name)
            or self.connect_client()
        )
        self.blocksize = (
            self.DEFAULT_BLOCK_SIZE if block_size in ["default", None] else block_size
        )
        self.loc = 0
        self.autocommit = autocommit
        self.end = None
        self.start = None
        self.closed = False

        if cache_options is None:
            cache_options = {}

        if "trim" in kwargs:
            warnings.warn(
                "Passing 'trim' to control the cache behavior has been deprecated. "
                "Specify it within the 'cache_options' argument instead.",
                FutureWarning,
            )
            cache_options["trim"] = kwargs.pop("trim")
        self.metadata = None
        self.kwargs = kwargs

        if self.mode not in {"ab", "rb", "wb"}:
            raise NotImplementedError("File mode not supported")
        if self.mode == "rb":
            if not hasattr(self, "details"):
                self.details = self.fs.info(self.path, version_id=self.version_id)
            elif self.fs.version_aware and (
                (self.version_id is None and not self.details.get("is_current_version"))
                or (
                    self.version_id is not None
                    and self.version_id != self.details.get("version_id")
                )
            ):
                self.details = self.fs.info(
                    self.path, version_id=self.version_id, refresh=True
                )
            self.size = self.details["size"]
            self.cache = caches[cache_type](
                blocksize=self.blocksize,
                fetcher=self._fetch_range,
                size=self.size,
                **cache_options,
            )
            self.metadata = sync(
                self.loop,
                get_blob_metadata,
                self.container_client,
                self.blob,
                version_id=self.version_id,
            )

        else:
            self.metadata = metadata or {"is_directory": "false"}
            self.buffer = io.BytesIO()
            self.offset = None
            self.forced = False
            self.location = None

    def close(self):
        """Close file and azure client."""
        asyncio.run_coroutine_threadsafe(close_container_client(self), loop=self.loop)
        super().close()

    def connect_client(self):
        """Connect to the Asynchronous BlobServiceClient, using user-specified connection details.
        Tries credentials first, then connection string and finally account key

        Raises
        ------
        ValueError if none of the connection details are available
        """
        try:
            if hasattr(self.fs, "account_host"):
                self.fs.account_url: str = f"https://{self.fs.account_host}"
            else:
                self.fs.account_url: str = (
                    f"https://{self.fs.account_name}.blob.core.windows.net"
                )

            creds = [self.fs.sync_credential, self.fs.account_key, self.fs.credential]
            if any(creds):
                self.container_client = [
                    AIOBlobServiceClient(
                        account_url=self.fs.account_url,
                        credential=cred,
                        _location_mode=self.fs.location_mode,
                    ).get_container_client(self.container_name)
                    for cred in creds
                    if cred is not None
                ][0]
            elif self.fs.connection_string is not None:
                self.container_client = AIOBlobServiceClient.from_connection_string(
                    conn_str=self.fs.connection_string
                ).get_container_client(self.container_name)
            elif self.fs.sas_token is not None:
                self.container_client = AIOBlobServiceClient(
                    account_url=self.fs.account_url + self.fs.sas_token, credential=None
                ).get_container_client(self.container_name)
            else:
                self.container_client = AIOBlobServiceClient(
                    account_url=self.fs.account_url
                ).get_container_client(self.container_name)

        except Exception as e:
            raise ValueError(
                f"Unable to fetch container_client with provided params for {e}!!"
            )

    async def _async_fetch_range(self, start: int, end: int = None, **kwargs):
        """
        Download a chunk of data specified by start and end

        Parameters
        ----------
        start: int
            Start byte position to download blob from
        end: int
            End of the file chunk to download
        """
        if end and (end > self.size):
            length = self.size - start
        else:
            length = None if end is None else (end - start)
        async with self.container_client:
            stream = await self.container_client.download_blob(
                blob=self.blob, offset=start, length=length, version_id=self.version_id
            )
            blob = await stream.readall()
        return blob

    _fetch_range = sync_wrapper(_async_fetch_range)

    async def _reinitiate_async_upload(self, **kwargs):
        pass

    async def _async_initiate_upload(self, **kwargs):
        """Prepare a remote file upload"""
        self._block_list = []
        if self.mode == "wb":
            try:
                await self.container_client.delete_blob(self.blob)
            except ResourceNotFoundError:
                pass
            except HttpResponseError:
                pass
            else:
                await self._reinitiate_async_upload()

        elif self.mode == "ab":
            if not await self.fs._exists(self.path):
                async with self.container_client.get_blob_client(blob=self.blob) as bc:
                    await bc.create_append_blob(metadata=self.metadata)
        else:
            raise ValueError(
                "File operation modes other than wb are not yet supported for writing"
            )

    _initiate_upload = sync_wrapper(_async_initiate_upload)

    def _get_chunks(self, data, chunk_size=1024**3):  # Keeping the chunk size as 1 GB
        start = 0
        length = len(data)
        while start < length:
            end = min(start + chunk_size, length)
            yield data[start:end]
            start = end

    async def _async_upload_chunk(self, final: bool = False, **kwargs):
        """
        Write one part of a multi-block file upload

        Parameters
        ----------
        final: bool
            This is the last block, so should complete file, if
            self.autocommit is True.

        """
        data = self.buffer.getvalue()
        length = len(data)
        block_id = len(self._block_list)
        block_id = f"{block_id:07d}"
        if self.mode == "wb":
            try:
                for chunk in self._get_chunks(data):
                    async with self.container_client.get_blob_client(
                        blob=self.blob
                    ) as bc:
                        await bc.stage_block(
                            block_id=block_id,
                            data=chunk,
                            length=len(chunk),
                        )
                        self._block_list.append(block_id)
                        block_id = len(self._block_list)
                        block_id = f"{block_id:07d}"

                if final:
                    block_list = [BlobBlock(_id) for _id in self._block_list]
                    async with self.container_client.get_blob_client(
                        blob=self.blob
                    ) as bc:
                        await bc.commit_block_list(
                            block_list=block_list, metadata=self.metadata
                        )
            except Exception as e:
                # This step handles the situation where data="" and length=0
                # which is throws an InvalidHeader error from Azure, so instead
                # of staging a block, we directly upload the empty blob
                # This isn't actually tested, since Azureite behaves differently.
                if block_id == "0000000" and length == 0 and final:
                    async with self.container_client.get_blob_client(
                        blob=self.blob
                    ) as bc:
                        await bc.upload_blob(data=data, metadata=self.metadata)
                elif length == 0 and final:
                    # just finalize
                    block_list = [BlobBlock(_id) for _id in self._block_list]
                    async with self.container_client.get_blob_client(
                        blob=self.blob
                    ) as bc:
                        await bc.commit_block_list(
                            block_list=block_list, metadata=self.metadata
                        )
                else:
                    raise RuntimeError(f"Failed to upload block: {e}!") from e
        elif self.mode == "ab":
            async with self.container_client.get_blob_client(blob=self.blob) as bc:
                await bc.upload_blob(
                    data=data,
                    length=length,
                    blob_type=BlobType.AppendBlob,
                    metadata=self.metadata,
                )
        else:
            raise ValueError(
                "File operation modes other than wb or ab are not yet supported for upload_chunk"
            )

    _upload_chunk = sync_wrapper(_async_upload_chunk)

    def __del__(self):
        try:
            if not self.closed:
                self.close()
        except TypeError:
            pass
