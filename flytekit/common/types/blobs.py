from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.types import base_sdk_types as _base_sdk_types
from flytekit.common.types.impl import blobs as _blob_impl
from flytekit.models import literals as _literals
from flytekit.models import types as _idl_types
from flytekit.models.core import types as _core_types


class BlobInstantiator(_base_sdk_types.InstantiableType):
    @staticmethod
    def create_at_known_location(location):
        """
        :param Text location:
        :rtype: flytekit.common.types.impl.blobs.Blob
        """
        return _blob_impl.Blob.create_at_known_location(location, mode="wb")

    @staticmethod
    def fetch(remote_path, local_path=None):
        """
        :param Text remote_path:
        :param Text local_path: [Optional] If specified, the Blob is copied to this location.  If specified,
            this location is NOT managed and the blob will not be cleaned up upon exit.
        :rtype: flytekit.common.types.impl.blobs.Blob
        """
        return _blob_impl.Blob.fetch(remote_path, mode="rb", local_path=local_path)

    def __call__(cls, *args, **kwargs):
        """
        TODO: Is there a better way to deal with this?

        We want the behavior of Types.Blob() returns a _blob_impl.Blob, but also to be able to use this object to
        wrap a _blob_impl.Blob via Types.Blob(_blob_impl.Blob()) for serialization, type checking, etc..

        :rtype: flytekit.common.types.impl.blobs.Blob
        """
        if not args and not kwargs:
            return _blob_impl.Blob.create_at_any_location(mode="wb")
        else:
            return super(BlobInstantiator, cls).__call__(*args, **kwargs)


# TODO: Make blobs and schemas pluggable
class Blob(_base_sdk_types.FlyteSdkValue, metaclass=BlobInstantiator):
    @classmethod
    def from_string(cls, string_value):
        """
        :param Text string_value:
        :rtype: Blob
        """
        if not string_value:
            _user_exceptions.FlyteValueException(string_value, "Cannot create a Blob from the provided path value.")
        return cls(_blob_impl.Blob.from_string(string_value, mode="rb"))

    @classmethod
    def is_castable_from(cls, other):
        """
        :param flytekit.common.types.base_literal_types.FlyteSdkType other:
        :rtype: bool
        """
        return cls == other

    @classmethod
    def from_python_std(cls, t_value):
        """
        :param T t_value: It is up to each individual object as to whether or not this value can be cast.
        :rtype: FlyteSdkValue
        :raises: flytekit.common.exceptions.user.FlyteTypeException
        """
        if t_value is None:
            return _base_sdk_types.Void()
        elif isinstance(t_value, _blob_impl.Blob):
            blob = t_value
        else:
            blob = _blob_impl.Blob.from_python_std(t_value)
        return cls(blob)

    @classmethod
    def to_flyte_literal_type(cls):
        """
        :rtype: flytekit.models.types.LiteralType
        """
        return _idl_types.LiteralType(
            blob=_core_types.BlobType(format="", dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE)
        )

    @classmethod
    def promote_from_model(cls, literal_model):
        """
        Creates an object of this type from the model primitive defining it.
        :param flytekit.models.literals.Literal literal_model:
        :rtype: Blob
        """
        return cls(_blob_impl.Blob.promote_from_model(literal_model.scalar.blob))

    @classmethod
    def short_class_string(cls):
        """
        :rtype: Text
        """
        return "Blob"

    def __init__(self, value):
        """
        :param flytekit.common.types.impl.blobs.Blob value: Blob value to wrap
        """
        super(Blob, self).__init__(scalar=_literals.Scalar(blob=value))

    def to_python_std(self):
        """
        :rtype: flytekit.common.types.impl.blobs.Blob
        """
        return self.scalar.blob

    def short_string(self):
        """
        :rtype: Text
        """
        return "Blob(uri={}{})".format(
            self.scalar.blob.uri,
            ", format={}".format(self.scalar.blob.metadata.type.format)
            if self.scalar.blob.metadata.type.format
            else "",
        )


class MultiPartBlobInstantiator(_base_sdk_types.InstantiableType):
    @staticmethod
    def create_at_known_location(location):
        """
        :param Text location:
        :rtype: flytekit.common.types.impl.blobs.MultiPartBlob
        """
        return _blob_impl.MultiPartBlob.create_at_known_location(location, mode="wb")

    @staticmethod
    def fetch(remote_path, local_path=None):
        """
        :param Text remote_path:
        :param Text local_path: [Optional] If specified, the MultiPartBlob is copied to this location.  If specified,
            this location is NOT managed and the blob will not be cleaned up upon exit.
        :rtype: flytekit.common.types.impl.blobs.MultiPartBlob
        """
        return _blob_impl.MultiPartBlob.fetch(remote_path, mode="rb", local_path=local_path)

    def __call__(cls, *args, **kwargs):
        """
        TODO: Is there a better way to deal with this?

        We want the behavior of Types.MultiPartBlob() returns a _blob_impl.MultiPartBlob, but also to be able to use
        this object to wrap a _blob_impl.MultiPartBlob via Types.MultiPartBlob(_blob_impl.MultiPartBlob()) for
        serialization, type checking, etc..

        :rtype: flytekit.common.types.impl.blobs.MultiPartBlob
        """
        if not args and not kwargs:
            return _blob_impl.MultiPartBlob.create_at_any_location(mode="wb")
        else:
            return super(MultiPartBlobInstantiator, cls).__call__(*args, **kwargs)


class MultiPartBlob(_base_sdk_types.FlyteSdkValue, metaclass=MultiPartBlobInstantiator):
    @classmethod
    def from_string(cls, string_value):
        """
        :param Text string_value:
        :rtype: MultiPartBlob
        """
        if not string_value:
            _user_exceptions.FlyteValueException(
                string_value, "Cannot create a MultiPartBlob from the provided path " "value.",
            )
        return cls(_blob_impl.MultiPartBlob.from_string(string_value, mode="rb"))

    @classmethod
    def is_castable_from(cls, other):
        """
        :param flytekit.common.types.base_literal_types.FlyteSdkType other:
        :rtype: bool
        """
        return cls == other

    @classmethod
    def from_python_std(cls, t_value):
        """
        :param T t_value: It is up to each individual object as to whether or not this value can be cast.
        :rtype: FlyteSdkValue
        :raises: flytekit.common.exceptions.user.FlyteTypeException
        """
        if t_value is None:
            return _base_sdk_types.Void()
        elif isinstance(t_value, _blob_impl.MultiPartBlob):
            blob = t_value
        else:
            blob = _blob_impl.MultiPartBlob.from_python_std(t_value)
        return cls(blob)

    @classmethod
    def to_flyte_literal_type(cls):
        """
        :rtype: flytekit.models.types.LiteralType
        """
        return _idl_types.LiteralType(
            blob=_core_types.BlobType(format="", dimensionality=_core_types.BlobType.BlobDimensionality.MULTIPART,)
        )

    @classmethod
    def promote_from_model(cls, literal_model):
        """
        Creates an object of this type from the model primitive defining it.
        :param flytekit.models.literals.Literal literal_model:
        :rtype: MultiPartBlob
        """
        return cls(_blob_impl.MultiPartBlob.promote_from_model(literal_model.scalar.blob))

    @classmethod
    def short_class_string(cls):
        """
        :rtype: Text
        """
        return "MultiPartBlob"

    def __init__(self, value):
        """
        :param flytekit.common.types.impl.blobs.MultiPartBlob value: Blob value to wrap
        """
        super(MultiPartBlob, self).__init__(scalar=_literals.Scalar(blob=value))

    def to_python_std(self):
        """
        :rtype: flytekit.common.types.impl.blobs.MultiPartBlob
        """
        return self.scalar.blob

    def short_string(self):
        """
        :rtype: Text
        """
        return "MultiPartBlob(uri={}{})".format(
            self.scalar.blob.uri,
            ", format={}".format(self.scalar.blob.metadata.type.format)
            if self.scalar.blob.metadata.type.format
            else "",
        )


class CsvInstantiator(BlobInstantiator):
    @staticmethod
    def create_at_known_location(location):
        """
        :param Text location:
        :rtype: flytekit.common.types.impl.blobs.CSV
        """
        return _blob_impl.Blob.create_at_known_location(location, mode="w", format="csv")

    @staticmethod
    def fetch(remote_path, local_path=None):
        """
        :param Text remote_path:
        :param Text local_path: [Optional] If specified, the MultiPartBlob is copied to this location.  If specified,
            this location is NOT managed and the blob will not be cleaned up upon exit.
        :rtype: flytekit.common.types.impl.blobs.CSV
        """
        return _blob_impl.Blob.fetch(remote_path, local_path=local_path, mode="r", format="csv")

    def __call__(cls, *args, **kwargs):
        """
        TODO: Is there a better way to deal with this?

        We want the behavior of Types.CSV() returns a _blob_impl.CSV, but also to be able to use
        this object to wrap a _blob_impl.CSV via Types.CSV(_blob_impl.CSV()) for
        serialization, type checking, etc..

        :rtype: flytekit.common.types.impl.blobs.CSV
        """
        if not args and not kwargs:
            return _blob_impl.Blob.create_at_any_location(mode="w", format="csv")
        else:
            return super(CsvInstantiator, cls).__call__(*args, **kwargs)


class CSV(Blob, metaclass=CsvInstantiator):
    @classmethod
    def from_string(cls, string_value):
        """
        :param Text string_value:
        :rtype: CSV
        """
        if not string_value:
            _user_exceptions.FlyteValueException(string_value, "Cannot create a CSV from the provided path value.")
        return cls(_blob_impl.Blob.from_string(string_value, format="csv", mode="r"))

    @classmethod
    def is_castable_from(cls, other):
        """
        :param flytekit.common.types.base_literal_types.FlyteSdkType other:
        :rtype: bool
        """
        return cls == other

    @classmethod
    def from_python_std(cls, t_value):
        """
        :param T t_value: It is up to each individual object as to whether or not this value can be cast.
        :rtype: FlyteSdkValue
        :raises: flytekit.common.exceptions.user.FlyteTypeException
        """
        if t_value is None:
            return _base_sdk_types.Void()
        elif isinstance(t_value, _blob_impl.Blob):
            if t_value.metadata.type.format != "csv":
                raise _user_exceptions.FlyteValueException(t_value, "Blob is in incorrect format.  Expected CSV.")
            blob = t_value
        else:
            blob = _blob_impl.Blob.from_python_std(t_value, format="csv", mode="w")
        return cls(blob)

    @classmethod
    def to_flyte_literal_type(cls):
        """
        :rtype: flytekit.models.types.LiteralType
        """
        return _idl_types.LiteralType(
            blob=_core_types.BlobType(format="csv", dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,)
        )

    @classmethod
    def promote_from_model(cls, literal_model):
        """
        Creates an object of this type from the model primitive defining it.
        :param flytekit.models.literals.Literal literal_model:
        :rtype: CSV
        """
        return cls(_blob_impl.Blob.promote_from_model(literal_model.scalar.blob, mode="r"))

    @classmethod
    def short_class_string(cls):
        """
        :rtype: Text
        """
        return "CSV"

    def __init__(self, value):
        """
        :param flytekit.common.types.impl.blobs.Blob value: CSV blob value to wrap
        """
        super(CSV, self).__init__(value)


class MultiPartCsvInstantiator(MultiPartBlobInstantiator):
    @staticmethod
    def create_at_known_location(location):
        """
        :param Text location:
        :rtype: flytekit.common.types.impl.blobs.MultiPartBlob
        """
        return _blob_impl.MultiPartBlob.create_at_known_location(location, mode="w", format="csv")

    @staticmethod
    def fetch(remote_path, local_path=None):
        """
        :param Text remote_path:
        :param Text local_path: [Optional] If specified, the MultiPartCSV is copied to this location.  If specified,
            this location is NOT managed and the blob will not be cleaned up upon exit.
        :rtype: flytekit.common.types.impl.blobs.MultiPartCSV
        """
        return _blob_impl.MultiPartBlob.fetch(remote_path, local_path=local_path, mode="r", format="csv")

    def __call__(cls, *args, **kwargs):
        """
        TODO: Is there a better way to deal with this?

        We want the behavior of Types.MultiPartCSV() returns a _blob_impl.MultiPartCSV, but also to be able to use
        this object to wrap a _blob_impl.MultiPartCSV via Types.MultiPartCSV(_blob_impl.MultiPartCSV()) for
        serialization, type checking, etc..

        :rtype: flytekit.common.types.impl.blobs.MultiPartCSV
        """
        if not args and not kwargs:
            return _blob_impl.MultiPartBlob.create_at_any_location(mode="w", format="csv")
        else:
            return super(MultiPartCsvInstantiator, cls).__call__(*args, **kwargs)


class MultiPartCSV(MultiPartBlob, metaclass=MultiPartCsvInstantiator):
    @classmethod
    def from_string(cls, string_value):
        """
        :param Text string_value:
        :rtype: MultiPartCSV
        """
        if not string_value:
            _user_exceptions.FlyteValueException(
                string_value, "Cannot create a MultiPartCSV from the provided path value.",
            )
        return cls(_blob_impl.MultiPartBlob.from_string(string_value, format="csv", mode="r"))

    @classmethod
    def is_castable_from(cls, other):
        """
        :param flytekit.common.types.base_literal_types.FlyteSdkType other:
        :rtype: bool
        """
        return cls == other

    @classmethod
    def from_python_std(cls, t_value):
        """
        :param T t_value: It is up to each individual object as to whether or not this value can be cast.
        :rtype: FlyteSdkValue
        :raises: flytekit.common.exceptions.user.FlyteTypeException
        """
        if t_value is None:
            return _base_sdk_types.Void()
        elif isinstance(t_value, _blob_impl.MultiPartBlob):
            if t_value.metadata.type.format != "csv":
                raise _user_exceptions.FlyteValueException(
                    t_value, "Multi Part Blob is in incorrect format.  Expected CSV."
                )
            blob = t_value
        else:
            blob = _blob_impl.MultiPartBlob.from_python_std(t_value, format="csv", mode="w")
        return cls(blob)

    @classmethod
    def to_flyte_literal_type(cls):
        """
        :rtype: flytekit.models.types.LiteralType
        """
        return _idl_types.LiteralType(
            blob=_core_types.BlobType(format="csv", dimensionality=_core_types.BlobType.BlobDimensionality.MULTIPART,)
        )

    @classmethod
    def promote_from_model(cls, literal_model):
        """
        Creates an object of this type from the model primitive defining it.
        :param flytekit.models.literals.Literal literal_model:
        :rtype: MultiPartCSV
        """
        return cls(_blob_impl.MultiPartBlob.promote_from_model(literal_model.scalar.blob, mode="r"))

    @classmethod
    def short_class_string(cls):
        """
        :rtype: Text
        """
        return "MultiPartCSV"

    def __init__(self, value):
        """
        :param flytekit.common.types.impl.blobs.MultiPartBlob value: MultiPartBlob value to wrap
        """
        super(MultiPartCSV, self).__init__(value)
