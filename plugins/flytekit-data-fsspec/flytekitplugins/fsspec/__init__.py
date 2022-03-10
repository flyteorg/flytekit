import importlib

from flytekit import StructuredDatasetTransformerEngine, logger
from flytekit.configuration import internal
from flytekit.types.structured.structured_dataset import GCS, S3

from .persist import FSSpecPersistence

if internal.LocalSDK.USE_STRUCTURED_DATASET.read():
    from .arrow import ArrowToParquetEncodingHandler, ParquetToArrowDecodingHandler
    from .pandas import PandasToParquetEncodingHandler, ParquetToPandasDecodingHandler

    def _register(protocol: str):
        logger.info(f"Registering fsspec {protocol} implementations and overriding default structured encoder/decoder.")
        StructuredDatasetTransformerEngine.register(PandasToParquetEncodingHandler(protocol), True, True)
        StructuredDatasetTransformerEngine.register(ParquetToPandasDecodingHandler(protocol), True, True)
        StructuredDatasetTransformerEngine.register(ArrowToParquetEncodingHandler(protocol), True, True)
        StructuredDatasetTransformerEngine.register(ParquetToArrowDecodingHandler(protocol), True, True)

    if importlib.util.find_spec("s3fs"):
        _register(S3)

    if importlib.util.find_spec("gcsfs"):
        _register(GCS)
