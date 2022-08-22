"""
Flytekit File Type
==========================================================
.. currentmodule:: flytekit.types.file

This list also contains a bunch of pre-formatted :py:class:`flytekit.types.file.FlyteFile` types.

.. autosummary::
   :toctree: generated/
   :template: file_types.rst

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
#: This can be used to denote that the returned file is of type hdf5 and can be received by other tasks that
#: accept an hdf5 format. This is usually useful for serializing Tensorflow models
HDF5EncodedFile = FlyteFile[hdf5]

html = typing.TypeVar("html")
#: Can be used to receive or return an HTMLPage. The underlying type is a FlyteFile type. This is just a
#: decoration and useful for attaching content type information with the file and automatically documenting code.
HTMLPage = FlyteFile[html]

joblib = typing.TypeVar("joblib")
#: This File represents a file that was serialized using `joblib.dump` method can be loaded back using `joblib.load`.
JoblibSerializedFile = FlyteFile[joblib]

jpeg = typing.TypeVar("jpeg")
#: Can be used to receive or return an JPEGImage. The underlying type is a FlyteFile type. This is just a
#: decoration and useful for attaching content type information with the file and automatically documenting code.
JPEGImageFile = FlyteFile[jpeg]

pdf = typing.TypeVar("pdf")
#: Can be used to receive or return an PDFFile. The underlying type is a FlyteFile type. This is just a
#: decoration and useful for attaching content type information with the file and automatically documenting code.
PDFFile = FlyteFile[pdf]

png = typing.TypeVar("png")
#: Can be used to receive or return an PNGImage. The underlying type is a FlyteFile type. This is just a
#: decoration and useful for attaching content type information with the file and automatically documenting code.
PNGImageFile = FlyteFile[png]

python_pickle = typing.TypeVar("python_pickle")
#: This type can be used when a serialized Python pickled object is returned and shared between tasks. This only
#: adds metadata to the file in Flyte, but does not really carry any object information.
PythonPickledFile = FlyteFile[python_pickle]

ipynb = typing.TypeVar("ipynb")
#: This type is used to identify a Python notebook file.
PythonNotebook = FlyteFile[ipynb]

svg = typing.TypeVar("svg")
#: Can be used to receive or return an SVGImage. The underlying type is a FlyteFile type. This is just a
#: decoration and useful for attaching content type information with the file and automatically documenting code.
SVGImageFile = FlyteFile[svg]

csv = typing.TypeVar("csv")
#: Can be used to receive or return a CSVFile. The underlying type is a FlyteFile type. This is just a
#: decoration and useful for attaching content type information with the file and automatically documenting code.
CSVFile = FlyteFile[csv]

onnx = typing.TypeVar("onnx")
#: Can be used to receive or return an ONNXFile. The underlying type is a FlyteFile type. This is just a
#: decoration and useful for attaching content type information with the file and automatically documenting code.
ONNXFile = FlyteFile[onnx]
