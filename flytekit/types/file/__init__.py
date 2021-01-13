import typing

from .file import FlyteFile

# The following section provides some predefined aliases for commonly used FlyteFile formats.
# This makes their usage extremely simple for the users. Please keep the list sorted.


HDF5EncodedFile = FlyteFile[typing.TypeVar("hdf5")]
"""
    This can be used to denote that the returned file is of type hdf5 and can be received by other tasks that
    accept an hdf5 format. This is usually useful for serializing Tensorflow models
"""

HTMLPage = FlyteFile[typing.TypeVar("html")]
"""
    Can be used to receive or return an PNGImage. The underlying type is a FlyteFile, type. This is just a
    decoration and useful for attaching content type information with the file and automatically documenting code.
"""

JoblibSerializedFile = FlyteFile[typing.TypeVar("joblib")]
"""
    This File represents a file that was serialized using `joblib.dump` method can be loaded back using `joblib.load`
"""

JPEGImageFile = FlyteFile[typing.TypeVar("jpeg")]
"""
    Can be used to receive or return an JPEGImage. The underlying type is a FlyteFile, type. This is just a
    decoration and useful for attaching content type information with the file and automatically documenting code.
"""

PDFFile = FlyteFile[typing.TypeVar("pdf")]
"""
    Can be used to receive or return an PDFFile. The underlying type is a FlyteFile, type. This is just a
    decoration and useful for attaching content type information with the file and automatically documenting code.
"""

PNGImageFile = FlyteFile[typing.TypeVar("png")]
"""
    Can be used to receive or return an PNGImage. The underlying type is a FlyteFile, type. This is just a
    decoration and useful for attaching content type information with the file and automatically documenting code.
"""

PythonPickledFile = FlyteFile[typing.TypeVar("python-pickle")]
"""
    This type can be used when a serialized python pickled object is returned and shared between tasks. This only
    adds metadata to the file in Flyte, but does not really carry any object information
"""

PythonNotebook = FlyteFile[typing.TypeVar("ipynb")]
"""
    This type is used to identify a python notebook file
"""

SVGImageFile = FlyteFile[typing.TypeVar("svg")]
"""
    Can be used to receive or return an SVGImage. The underlying type is a FlyteFile, type. This is just a
    decoration and useful for attaching content type information with the file and automatically documenting code.
"""
