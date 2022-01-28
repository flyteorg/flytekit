import os
import sys
from importlib import reload

SD_MODULE_NAME = "flytekit.types.structured.structured_dataset"
SD_PACKAGE_NAME = "flytekit.types.structured"


def pytest_sessionstart():
    os.environ["FLYTE_SDK_USE_STRUCTURED_DATASET_OLD"] = os.environ.get("FLYTE_SDK_USE_STRUCTURED_DATASET_OLD", "FALSE")
    os.environ["FLYTE_SDK_USE_STRUCTURED_DATASET"] = "TRUE"
    # Use Structured Datasets. This should get picked up automatically by the loading of structured_dataset.py
    # If not in sys.modules, that means the Python process hasn't loaded it yet so we don't have to reload it
    if SD_MODULE_NAME in sys.modules:
        reload(sys.modules[SD_MODULE_NAME])
        reload(sys.modules[SD_PACKAGE_NAME])


def pytest_sessionfinish():
    os.environ["FLYTE_SDK_USE_STRUCTURED_DATASET"] = os.environ["FLYTE_SDK_USE_STRUCTURED_DATASET_OLD"]
