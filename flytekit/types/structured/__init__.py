from flytekit.loggers import logger

from .parquet import (
    ArrowToParquetPersistenceHandlers,
    PandasToParquetPersistenceHandlers,
    ParquetToArrowRetrievalHandler,
    ParquetToPandasRetrievalHandler,
)

try:
    from .bigquery import (
        ArrowToBQPersistenceHandlers,
        BQToArrowRetrievalHandler,
        BQToPandasRetrievalHandler,
        PandasToBQPersistenceHandlers,
    )
except ImportError:
    logger.info(
        "We won't register bigquery handler for structured dataset because "
        "we can't find the packages google-cloud-bigquery-storage and google-cloud-bigquery"
    )
