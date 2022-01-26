import os


def pytest_sessionstart():
    # Use Structured Datasets. This should get picked up automatically by the
    os.environ["FLYTE_SDK_USE_STRUCTURED_DATASET"] = "TRUE"
