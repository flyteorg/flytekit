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


def register_handlers(python_type: str):
    """
    Register handlers for structured dataset
    """
    if python_type == "pandas":
        import pandas as pd

        StructuredDatasetTransformerEngine.register(PandasToParquetEncodingHandler(), default_format_for_type=True)
        StructuredDatasetTransformerEngine.register(ParquetToPandasDecodingHandler(), default_format_for_type=True)
        StructuredDatasetTransformerEngine.register(PandasToBQEncodingHandlers())
        StructuredDatasetTransformerEngine.register(BQToPandasDecodingHandler())
        StructuredDatasetTransformerEngine.register_renderer(pd.DataFrame, TopFrameRenderer())
    elif python_type == "pyarrow":
        import pyarrow as pa

        StructuredDatasetTransformerEngine.register(ArrowToParquetEncodingHandler(), default_format_for_type=True)
        StructuredDatasetTransformerEngine.register(ParquetToArrowDecodingHandler(), default_format_for_type=True)
        StructuredDatasetTransformerEngine.register(ArrowToBQEncodingHandlers())
        StructuredDatasetTransformerEngine.register(BQToArrowDecodingHandler())
        StructuredDatasetTransformerEngine.register_renderer(pa.Table, ArrowRenderer())
