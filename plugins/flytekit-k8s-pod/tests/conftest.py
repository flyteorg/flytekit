import os


def pytest_sessionstart():
    os.environ["FLYTE_SDK_USE_STRUCTURED_DATASET_OLD"] = os.environ.get("FLYTE_SDK_USE_STRUCTURED_DATASET_OLD", "FALSE")
    # Use Structured Datasets. This should get picked up automatically by the loading of structured_dataset.py
    os.environ["FLYTE_SDK_USE_STRUCTURED_DATASET"] = "TRUE"


def pytest_sessionfinish():
    os.environ["FLYTE_SDK_USE_STRUCTURED_DATASET"] = os.environ["FLYTE_SDK_USE_STRUCTURED_DATASET_OLD"]
