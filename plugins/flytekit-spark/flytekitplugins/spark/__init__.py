"""
====================
Spark
====================
Spark plugin

.. currentmodule:: flytekitplugins.spark

.. autosummary::

   Spark
   PysparkFunctionTask
   new_spark_session
   SparkDataFrameSchemaReader
   SparkDataFrameSchemaWriter
   SparkDataFrameTransformer
   ParquetToSparkDecodingHandler
   SparkToParquetEncodingHandler

"""


__all__ = [
    "Spark",
    "PysparkFunctionTask",
    "new_spark_session",
    "SparkDataFrameSchemaReader",
    "SparkDataFrameSchemaWriter",
    "SparkDataFrameTransformer",
    "SparkToParquetEncodingHandler",
    "ParquetToSparkDecodingHandler",
]


from flytekit.configuration import internal as _internal

from .schema import SparkDataFrameSchemaReader, SparkDataFrameSchemaWriter, SparkDataFrameTransformer  # noqa
from .sd_transformers import ParquetToSparkDecodingHandler, SparkToParquetEncodingHandler
from .task import Spark, new_spark_session  # noqa
