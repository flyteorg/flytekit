"""
Flytekit File Type
==========================================================
.. currentmodule:: flytekit.types.file

This list also contains a bunch of pre-formatted :py:class:`flytekit.types.file.FlyteFile` types.

.. autosummary::
   :toctree: generated/

   FlyteFile
   HDF5EncodedFile
   HTMLPage
   JoblibSerializedFile
   JPEGImageFile
   PDFFile
   PNGImageFile
   PythonPickledFile
   PythonNotebook
   SVGImageFile
"""

import typing

from .file import FlyteFile

# The following section provides some predefined aliases for commonly used FlyteFile formats.
# This makes their usage extremely simple for the users. Please keep the list sorted.


hdf5 = typing.TypeVar("hdf5")
HDF5EncodedFile = FlyteFile[hdf5]
"""
    This can be used to denote that the returned file is of type hdf5 and can be received by other tasks that
    accept an hdf5 format. This is usually useful for serializing Tensorflow models
"""

html = typing.TypeVar("html")
HTMLPage = FlyteFile[html]
"""
    Can be used to receive or return an PNGImage. The underlying type is a FlyteFile, type. This is just a
    decoration and useful for attaching content type information with the file and automatically documenting code.
"""

joblib = typing.TypeVar("joblib")
JoblibSerializedFile = FlyteFile[joblib]
"""
    This File represents a file that was serialized using `joblib.dump` method can be loaded back using `joblib.load`
"""

jpeg = typing.TypeVar("jpeg")
JPEGImageFile = FlyteFile[jpeg]
"""
    Can be used to receive or return an JPEGImage. The underlying type is a FlyteFile, type. This is just a
    decoration and useful for attaching content type information with the file and automatically documenting code.
"""

pdf = typing.TypeVar("pdf")
PDFFile = FlyteFile[pdf]
"""
    Can be used to receive or return an PDFFile. The underlying type is a FlyteFile, type. This is just a
    decoration and useful for attaching content type information with the file and automatically documenting code.
"""

png = typing.TypeVar("png")
PNGImageFile = FlyteFile[png]
"""
    Can be used to receive or return an PNGImage. The underlying type is a FlyteFile, type. This is just a
    decoration and useful for attaching content type information with the file and automatically documenting code.
"""

python_pickle = typing.TypeVar("python_pickle")
PythonPickledFile = FlyteFile[python_pickle]
"""
    This type can be used when a serialized python pickled object is returned and shared between tasks. This only
    adds metadata to the file in Flyte, but does not really carry any object information
"""

ipynb = typing.TypeVar("ipynb")
PythonNotebook = FlyteFile[ipynb]
"""
    This type is used to identify a python notebook file
"""

svg = typing.TypeVar("svg")
SVGImageFile = FlyteFile[svg]
"""
    Can be used to receive or return an SVGImage. The underlying type is a FlyteFile, type. This is just a
    decoration and useful for attaching content type information with the file and automatically documenting code.
"""

csv = typing.TypeVar("csv")
CSVFile = FlyteFile[csv]
"""
    Can be used to receive or return a CSVFile. The underlying type is a FlyteFile, type. This is just a
    decoration and useful for attaching content type information with the file and automatically documenting code.
"""
