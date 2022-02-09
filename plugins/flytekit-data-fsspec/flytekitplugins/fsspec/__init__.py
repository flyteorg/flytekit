from flytekit import USE_STRUCTURED_DATASET

from .persist import FSSpecPersistence

if USE_STRUCTURED_DATASET.get():
    from .basic_dfs import (
        ArrowToParquetEncodingHandler,
        PandasToParquetEncodingHandler,
        ParquetToArrowDecodingHandler,
        ParquetToPandasDecodingHandler,
    )
