import importlib

from flytekit import USE_STRUCTURED_DATASET, StructuredDatasetTransformerEngine, logger
from flytekit.types.structured.structured_dataset import S3

from .persist import FSSpecPersistence

if USE_STRUCTURED_DATASET.get():
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
