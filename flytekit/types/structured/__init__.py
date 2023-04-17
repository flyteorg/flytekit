"""
Flytekit StructuredDataset
==========================================================
.. currentmodule:: flytekit.types.structured

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   StructuredDataset
   StructuredDatasetEncoder
   StructuredDatasetDecoder
"""


from flytekit.configuration.internal import LocalSDK
from flytekit.deck.renderer import ArrowRenderer, TopFrameRenderer
from flytekit.loggers import logger

from .basic_dfs import (
    ArrowToParquetEncodingHandler,
    PandasToParquetEncodingHandler,
    ParquetToArrowDecodingHandler,
    ParquetToPandasDecodingHandler,
)
from .structured_dataset import (
    StructuredDataset,
    StructuredDatasetDecoder,
    StructuredDatasetEncoder,
    StructuredDatasetTransformerEngine,
)

try:
    from .bigquery import (
        ArrowToBQEncodingHandlers,
        BQToArrowDecodingHandler,
        BQToPandasDecodingHandler,
        PandasToBQEncodingHandlers,
    )
except ImportError:
    logger.info(
        "We won't register bigquery handler for structured dataset because "
        "we can't find the packages google-cloud-bigquery-storage and google-cloud-bigquery"
    )


def register_pandas_handlers():
    import pandas as pd

    StructuredDatasetTransformerEngine.register(PandasToParquetEncodingHandler(), default_format_for_type=True)
    StructuredDatasetTransformerEngine.register(ParquetToPandasDecodingHandler(), default_format_for_type=True)
    StructuredDatasetTransformerEngine.register_renderer(pd.DataFrame, TopFrameRenderer())


def register_arrow_handlers():
    import pyarrow as pa

    StructuredDatasetTransformerEngine.register(ArrowToParquetEncodingHandler(), default_format_for_type=True)
    StructuredDatasetTransformerEngine.register(ParquetToArrowDecodingHandler(), default_format_for_type=True)
    StructuredDatasetTransformerEngine.register_renderer(pa.Table, ArrowRenderer())


def register_bigquery_handlers():
    StructuredDatasetTransformerEngine.register(PandasToBQEncodingHandlers())
    StructuredDatasetTransformerEngine.register(BQToPandasDecodingHandler())
    StructuredDatasetTransformerEngine.register(ArrowToBQEncodingHandlers())
    StructuredDatasetTransformerEngine.register(BQToArrowDecodingHandler())
