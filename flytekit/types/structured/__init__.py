"""
Flytekit StructuredDataset
==========================================================
.. currentmodule:: flytekit.types.structured

.. autosummary::
   :template: custom.rst
   :toctree: generated/

    StructuredDataset
    StructuredDatasetDecoder
    StructuredDatasetEncoder
"""

from flytekit.deck.renderer import ArrowRenderer, TopFrameRenderer
from flytekit.lazy_import.lazy_module import is_imported
from flytekit.loggers import logger

from .structured_dataset import (
    DuplicateHandlerError,
    StructuredDataset,
    StructuredDatasetDecoder,
    StructuredDatasetEncoder,
    StructuredDatasetTransformerEngine,
)


def register_csv_handlers():
    from .basic_dfs import CSVToPandasDecodingHandler, PandasToCSVEncodingHandler

    StructuredDatasetTransformerEngine.register(PandasToCSVEncodingHandler(), default_format_for_type=True)
    StructuredDatasetTransformerEngine.register(CSVToPandasDecodingHandler(), default_format_for_type=True)


def register_pandas_handlers():
    import pandas as pd

    from .basic_dfs import PandasToParquetEncodingHandler, ParquetToPandasDecodingHandler

    StructuredDatasetTransformerEngine.register(PandasToParquetEncodingHandler(), default_format_for_type=True)
    StructuredDatasetTransformerEngine.register(ParquetToPandasDecodingHandler(), default_format_for_type=True)
    StructuredDatasetTransformerEngine.register_renderer(pd.DataFrame, TopFrameRenderer())


def register_arrow_handlers():
    import pyarrow as pa

    from .basic_dfs import ArrowToParquetEncodingHandler, ParquetToArrowDecodingHandler

    StructuredDatasetTransformerEngine.register(ArrowToParquetEncodingHandler(), default_format_for_type=True)
    StructuredDatasetTransformerEngine.register(ParquetToArrowDecodingHandler(), default_format_for_type=True)
    StructuredDatasetTransformerEngine.register_renderer(pa.Table, ArrowRenderer())


def register_bigquery_handlers():
    try:
        from .bigquery import (
            ArrowToBQEncodingHandlers,
            BQToArrowDecodingHandler,
            BQToPandasDecodingHandler,
            PandasToBQEncodingHandlers,
        )

        StructuredDatasetTransformerEngine.register(PandasToBQEncodingHandlers())
        StructuredDatasetTransformerEngine.register(BQToPandasDecodingHandler())
        StructuredDatasetTransformerEngine.register(ArrowToBQEncodingHandlers())
        StructuredDatasetTransformerEngine.register(BQToArrowDecodingHandler())
    except ImportError:
        logger.info(
            "We won't register bigquery handler for structured dataset because "
            "we can't find the packages google-cloud-bigquery-storage and google-cloud-bigquery"
        )


def register_snowflake_handlers():
    try:
        from .snowflake import PandasToSnowflakeEncodingHandlers, SnowflakeToPandasDecodingHandler

        StructuredDatasetTransformerEngine.register(SnowflakeToPandasDecodingHandler())
        StructuredDatasetTransformerEngine.register(PandasToSnowflakeEncodingHandlers())

    except ImportError:
        logger.info(
            "We won't register snowflake handler for structured dataset because "
            "we can't find package snowflake-connector-python"
        )


def lazy_import_structured_dataset_handler():
    if is_imported("pandas"):
        try:
            register_pandas_handlers()
            register_csv_handlers()
        except DuplicateHandlerError:
            logger.debug("Transformer for pandas is already registered.")
    if is_imported("pyarrow"):
        try:
            register_arrow_handlers()
        except DuplicateHandlerError:
            logger.debug("Transformer for arrow is already registered.")
    if is_imported("google.cloud.bigquery"):
        try:
            register_bigquery_handlers()
        except DuplicateHandlerError:
            logger.debug("Transformer for bigquery is already registered.")
    if is_imported("snowflake.connector"):
        try:
            register_snowflake_handlers()
        except DuplicateHandlerError:
            logger.debug("Transformer for snowflake is already registered.")
