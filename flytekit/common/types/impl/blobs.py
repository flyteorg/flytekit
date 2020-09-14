import os as _os
import shutil as _shutil
import sys as _sys
import uuid as _uuid

import six as _six

from flytekit.common import sdk_bases as _sdk_bases
from flytekit.common import utils as _utils
from flytekit.common.exceptions import scopes as _exception_scopes
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.interfaces.data import data_proxy as _data_proxy
from flytekit.models import literals as _literal_models
from flytekit.models.core import types as _core_types


class Blob(_literal_models.Blob, metaclass=_sdk_bases.ExtendedSdkType):
    def __init__(self, remote_path, mode="rb", format=None):
        """
        :param Text remote_path: Path to location where the Blob should be synced to.
        :param Text mode: File access mode.  'a' and '+' are forbidden.  A blob can only be written or read at a time.
        :param Text format: Format
        """
        if "+" in mode or "a" in mode or ("w" in mode and "r" in mode):
            raise _user_exceptions.FlyteAssertion("A blob cannot be read and written at the same time")
        self._mode = mode
        self._local_path = None
        self._file = None
        super(Blob, self).__init__(
            _literal_models.BlobMetadata(
                type=_core_types.BlobType(format or "", _core_types.BlobType.BlobDimensionality.SINGLE)
            ),
            remote_path,
        )

    @classmethod
    @_exception_scopes.system_entry_point
    def from_python_std(cls, t_value, mode="wb", format=None):
        """
        :param T t_value:
        :param Text mode: File access mode.  'a' and '+' are forbidden.  A blob can only be written or read at a time.
        :param Text format:
        :rtype: Blob
        """
        if isinstance(t_value, (_six.text_type, str)):
            if _os.path.isfile(t_value):
                blob = cls.create_at_any_location(mode=mode, format=format)
                blob._local_path = t_value
                blob.upload()
            else:
                blob = cls.create_at_known_location(t_value, mode=mode, format=format)
            return blob
        elif isinstance(t_value, cls):
            return t_value
        else:
            raise _user_exceptions.FlyteTypeException(
                type(t_value),
                {_six.text_type, str, Blob},
                received_value=t_value,
                additional_msg="Unable to create Blob from user-provided value.",
            )

    @classmethod
    @_exception_scopes.system_entry_point
    def from_string(cls, t_value, mode="wb", format=None):
        """
        :param T t_value:
        :param Text mode: Read or write mode of the object.
        :param Text format:
        :rtype: Blob
        """
        return cls.create_at_known_location(t_value, mode=mode, format=format)

    @classmethod
    @_exception_scopes.system_entry_point
    def create_at_known_location(cls, known_remote_location, mode="wb", format=None):
        """
        :param Text known_remote_location: The location to which to write the object.  Usually an s3 path.
        :param Text mode:
        :param Text format:
        :rtype: Blob
        """
        return cls(known_remote_location, mode=mode, format=format)

    @classmethod
    @_exception_scopes.system_entry_point
    def create_at_any_location(cls, mode="wb", format=None):
        """
        :param Text mode:
        :param Text format:
        :rtype: Blob
        """
        return cls.create_at_known_location(_data_proxy.Data.get_remote_path(), mode=mode, format=format)

    @classmethod
    @_exception_scopes.system_entry_point
    def fetch(cls, remote_path, local_path=None, overwrite=False, mode="rb", format=None):
        """
        :param Text remote_path: The location from which to fetch the object. Usually an s3 path.
        :param Text local_path: [Optional] A local path to which to download the object. If specified, the object
            will not be managed and might not be cleaned up by the system upon exiting the context.
        :param bool overwrite: If True, objects will be overwritten at the provided local_path in order to fetch this
            object.  Default is False.
        :param Text mode: Read or write mode of the object.
        :param Text format: Format the object is in.
        :rtype: Blob
        """
        blob = cls(remote_path, mode=mode, format=format)
        blob.download(local_path=local_path, overwrite=overwrite)
        return blob

    @classmethod
    def promote_from_model(cls, model, mode="rb"):
        """
        :param flytekit.models.literals.Blob model:
        :param Text mode: Read or write mode of the object.
        :rtype: Blob
        """
        return cls(model.uri, format=model.metadata.type.format, mode=mode)

    @property
    def local_path(self):
        """
        Local filesystem path where the file was downloaded
        :rtype: Text
        """
        return self._local_path

    @property
    def remote_location(self):
        """
        Path to where this Blob will be synced.
        :rtype: Text
        """
        return self.uri

    @property
    def mode(self):
        """
        The mode string the Blob is associated with.
        :rtype: Text
        """
        return self._mode

    @_exception_scopes.system_entry_point
    def __enter__(self):
        """
        :rtype: typing.BinaryIO
        """
        if self._file is not None:
            raise _user_exceptions.FlyteAssertion("Only one reference can be open to a blob at a time.")

        if self.local_path is None:
            if "r" in self.mode:
                self.download()
            elif "w" in self.mode:
                self._generate_local_path()

        self._file = open(self.local_path, self.mode)
        return self._file

    @_exception_scopes.system_entry_point
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._file is not None and not self._file.closed:
            self._file.close()
            self._file = None
            if "w" in self.mode:
                self.upload()
        return False

    def _generate_local_path(self):
        if _data_proxy.LocalWorkingDirectoryContext.get() is None:
            raise _user_exceptions.FlyteAssertion(
                "No temporary file system is present.  Either call this method from within the "
                "context of a task or surround with a 'with LocalTestFileSystem():' block.  Or "
                "specify a path when calling this function.  Note: Cleanup is not automatic when a "
                "path is specified."
            )
        self._local_path = _data_proxy.LocalWorkingDirectoryContext.get().get_named_tempfile(_uuid.uuid4().hex)

    @_exception_scopes.system_entry_point
    def download(self, local_path=None, overwrite=False):
        """
        Alternate method, rather than the context manager interface to download the binary file to the local disk.
        :param Text local_path: [Optional] If provided, the blob will be downloaded to this path.  This will make the
            resulting file object unmanaged and it will not be cleaned up by the system upon exiting the context.
        :param bool overwrite: If true and local_path is specified, we will download the blob and
            overwrite an existing file at that location.  Default is False.
        """
        if "r" not in self._mode:
            raise _user_exceptions.FlyteAssertion("Cannot download a write-only blob!")

        if local_path:
            self._local_path = local_path

        if not self.local_path:
            self._generate_local_path()

        if overwrite or not _os.path.exists(self.local_path):
            # TODO: Introduce system logging
            # logging.info("Getting {} -> {}".format(self.remote_location, self.local_path))
            _data_proxy.Data.get_data(self.remote_location, self.local_path, is_multipart=False)
        else:
            raise _user_exceptions.FlyteAssertion(
                "Cannot download blob to a location that already exists when overwrite is not set to True.  "
                "Attempted download from {} -> {}".format(self.remote_location, self.local_path)
            )

    @_exception_scopes.system_entry_point
    def upload(self):
        """
        Upload the blob to the remote location
        """
        if "w" not in self.mode:
            raise _user_exceptions.FlyteAssertion("Cannot upload a read-only blob!")

        elif not self.local_path:
            raise _user_exceptions.FlyteAssertion(
                "The Blob is not currently backed by a local file and therefore "
                "cannot be uploaded.  Please write to this Blob before attempting "
                "an upload."
            )
        else:
            # TODO: Introduce system logging
            # logging.info("Putting {} -> {}".format(self.local_path, self.remote_location))
            _data_proxy.Data.put_data(self.local_path, self.remote_location, is_multipart=False)


class MultiPartBlob(_literal_models.Blob, metaclass=_sdk_bases.ExtendedSdkType):
    def __init__(self, remote_path, mode="rb", format=None):
        """
        :param Text remote_path: Path to location where the Blob should be synced to.
        :param Text mode: File access mode.  'a' and '+' are forbidden.  A blob can only be written or read at a time.
        :param Text format: Format of underlying blob pieces.
        """
        remote_path = remote_path.strip().rstrip("/") + "/"
        super(MultiPartBlob, self).__init__(
            _literal_models.BlobMetadata(
                type=_core_types.BlobType(format or "", _core_types.BlobType.BlobDimensionality.MULTIPART)
            ),
            remote_path,
        )
        self._is_managed = False
        self._blobs = []
        self._directory = None
        self._mode = mode

    @classmethod
    def promote_from_model(cls, model, mode="rb"):
        """
        :param flytekit.models.literals.Blob model:
        :param Text mode: File access mode.  'a' and '+' are forbidden.  A blob can only be written or read at a time.
        :rtype: Blob
        """
        return cls(model.uri, format=model.metadata.type.format, mode=mode)

    @classmethod
    @_exception_scopes.system_entry_point
    def create_at_known_location(cls, known_remote_location, mode="wb", format=None):
        """
        :param Text known_remote_location: The location to which to write the object.  Usually an s3 path.
        :param Text mode:
        :param Text format:
        :rtype: MultiPartBlob
        """
        return cls(known_remote_location, mode=mode, format=format)

    @classmethod
    @_exception_scopes.system_entry_point
    def create_at_any_location(cls, mode="wb", format=None):
        """
        :param Text mode:
        :param Text format:
        :rtype: MultiPartBlob
        """
        return cls.create_at_known_location(_data_proxy.Data.get_remote_path(), mode=mode, format=format)

    @classmethod
    @_exception_scopes.system_entry_point
    def fetch(cls, remote_path, local_path=None, overwrite=False, mode="rb", format=None):
        """
        :param Text remote_path: The location from which to fetch the object. Usually an s3 path.
        :param Text local_path: [Optional] A local path to which to download the object. If specified, the object
            will not be managed and might not be cleaned up by the system upon exiting the context.
        :param bool overwrite: If True, objects will be overwritten at the provided local_path in order to fetch this
            object.  Default is False.
        :param Text mode: Read or write mode of the object.
        :param Text format: Format the object is in.
        :rtype: MultiPartBlob
        """
        blob = cls(remote_path, mode=mode, format=format)
        blob.download(local_path=local_path, overwrite=overwrite)
        return blob

    @classmethod
    @_exception_scopes.system_entry_point
    def from_python_std(cls, t_value, mode="wb", format=None):
        """
        :param T t_value:
        :param Text mode: Read or write mode of the object.
        :param Text format:
        :rtype: MultiPartBlob
        """
        if isinstance(t_value, (str, _six.text_type)):
            if _os.path.isdir(t_value):
                # TODO: Infer format
                blob = cls.create_at_any_location(mode=mode, format=format)
                blob._directory = _utils.Directory(t_value)
                blob.upload()
            else:
                blob = cls.create_at_known_location(t_value, mode=mode, format=format)
            return blob
        elif isinstance(t_value, cls):
            return t_value
        else:
            raise _user_exceptions.FlyteTypeException(
                type(t_value),
                {str, _six.text_type, MultiPartBlob},
                received_value=t_value,
                additional_msg="Unable to create Blob from user-provided value.",
            )

    @classmethod
    @_exception_scopes.system_entry_point
    def from_string(cls, t_value, mode="wb", format=None):
        """
        :param T t_value:
        :param Text mode: Read or write mode of the object.
        :param Text format:
        :rtype: MultiPartBlob
        """
        return cls.create_at_known_location(t_value, mode=mode, format=format)

    @_exception_scopes.system_entry_point
    def __enter__(self):
        """
        :rtype: list[typing.BinaryIO]
        """
        if "r" not in self.mode:
            raise _user_exceptions.FlyteAssertion("Do not enter context to write to directory. Call create_piece")

        try:
            if not self._directory:
                if _data_proxy.LocalWorkingDirectoryContext.get() is None:
                    raise _user_exceptions.FlyteAssertion(
                        "No temporary file system is present.  Either call this method from within the "
                        "context of a task or surround with a 'with LocalTestFileSystem():' block.  Or "
                        "specify a path when calling this function.  Note: Cleanup is not automatic when a "
                        "path is specified."
                    )
                self._directory = _utils.AutoDeletingTempDir(
                    _uuid.uuid4().hex, tmp_dir=_data_proxy.LocalWorkingDirectoryContext.get().name,
                )
                self._is_managed = True
                self._directory.__enter__()
                # TODO: Introduce system logging
                # logging.info("Copying recursively {} -> {}".format(self.remote_location, self.local_path))
                _data_proxy.Data.get_data(self.remote_location, self.local_path, is_multipart=True)

            # Read the files into blobs in case-insensitive lexicographically ascending orders
            self._blobs = []
            file_handles = []
            for local_path in sorted(self._directory.list_dir(), key=lambda x: x.lower()):
                b = Blob(_os.path.join(self.remote_location, _os.path.basename(local_path)), mode=self.mode,)
                b._local_path = local_path
                file_handles.append(b.__enter__())
                self._blobs.append(b)

            return file_handles
        except Exception:
            # Exit is idempotent so close partially opened context that way
            exc_type, exc_obj, exc_tb = _sys.exc_info()
            self.__exit__(exc_type, exc_obj, exc_tb)
            raise

    @_exception_scopes.system_entry_point
    def __exit__(self, exc_type, exc_val, exc_tb):
        for blob in self._blobs:
            blob.__exit__(exc_type, exc_val, exc_tb)
        self._blobs = []
        if self._is_managed:
            self._directory.__exit__(exc_type, exc_val, exc_tb)
            self._directory = None
            self._is_managed = False
        return False

    @property
    def local_path(self):
        """
        Local filesystem path where the file was downloaded
        :rtype: Text
        """
        if not self._directory:
            return None
        return self._directory.name

    @property
    def remote_location(self):
        """
        Path to where this MultiPartBlob will be synced.
        :rtype: Text
        """
        return self.uri

    @property
    def mode(self):
        """
        The mode string the MultiPartBlob is associated with.
        :rtype: Text
        """
        return self._mode

    @_exception_scopes.system_entry_point
    def create_part(self, name=None):
        """
        Method which will return a Blob object for writing into a multi-part blob.

        :param Text name: [optional] If name is provided, it is a specific partition name to place as part of the
            multipart blob.  When we read blobs from a multipart, it will be read in lexicographic order so this can be
            used to enforce ordering.  If not provided, the name is randomly generated.
        :rtype: Blob
        """
        if "w" not in self.mode:
            raise _user_exceptions.FlyteAssertion("Cannot create a blob in a read-only multipart blob")
        if name is None:
            name = _uuid.uuid4().hex
        if ":" in name or "/" in name:
            raise _user_exceptions.FlyteAssertion(
                name, "Cannot create a part of a multi-part object with ':' or '/' in the name.",
            )
        return Blob.create_at_known_location(
            _os.path.join(self.remote_location, name), mode=self.mode, format=self.metadata.type.format,
        )

    @_exception_scopes.system_entry_point
    def download(self, local_path=None, overwrite=False):
        """
        Forces the download of the remote multi-part blob to the local machine.
        :param Text local_path: [Optional] If provided, the blob pieces will be downloaded to this path.  This will
            make the resulting file objects unmanaged and it will not be cleaned up by the system upon exiting the
            context.
        :param bool overwrite: If true and local_path is specified, we will download the blob pieces and
            overwrite any existing files at that location.  Default is False.
        """
        if "r" not in self.mode:
            raise _user_exceptions.FlyteAssertion("Cannot download a write-only object!")

        if local_path:
            self._is_managed = False
        elif _data_proxy.LocalWorkingDirectoryContext.get() is None:
            raise _user_exceptions.FlyteAssertion(
                "No temporary file system is present.  Either call this method from within the "
                "context of a task or surround with a 'with LocalTestFileSystem():' block.  Or "
                "specify a path when calling this function.  Note: Cleanup is not automatic when a "
                "path is specified."
            )
        else:
            local_path = _data_proxy.LocalWorkingDirectoryContext.get().get_named_tempfile(_uuid.uuid4().hex)

        path_exists = _os.path.exists(local_path)
        if not path_exists or overwrite:
            if path_exists:
                _shutil.rmtree(local_path)
            _os.makedirs(local_path)
            self._directory = _utils.Directory(local_path)
            _data_proxy.Data.get_data(self.remote_location, self.local_path, is_multipart=True)
        else:
            raise _user_exceptions.FlyteAssertion(
                "Cannot download multi-part blob to a location that already exists when overwrite is not set to True. "
                "Attempted download from {} -> {}".format(self.remote_location, self.local_path)
            )

    @_exception_scopes.system_entry_point
    def upload(self):
        """
        Upload the multi-part blob to the remote location
        """
        if "w" not in self.mode:
            raise _user_exceptions.FlyteAssertion("Cannot upload a read-only multi-part blob!")

        elif not self.local_path:
            raise _user_exceptions.FlyteAssertion(
                "The multi-part blob is not currently backed by a local directoru "
                "and therefore cannot be uploaded.  Please write to this before "
                "attempting an upload."
            )
        else:
            # TODO: Introduce system logging
            # logging.info("Putting {} -> {}".format(self.local_path, self.remote_location))
            _data_proxy.Data.put_data(self.local_path, self.remote_location, is_multipart=True)
