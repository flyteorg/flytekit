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
   StructuredDatasetTransformerEngine
"""


from flytekit.configuration.internal import LocalSDK
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
