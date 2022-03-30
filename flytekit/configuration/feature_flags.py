import os


def _get(key: str, default_val: str) -> str:
    v = os.environ.get(key)
    return v if v else default_val


class FeatureFlags:
    FLYTE_PYTHON_PACKAGE_ROOT = _get("FLYTE_PYTHON_PACKAGE_ROOT", "auto")
    """
    Valid values, "auto", "." or an actual path
    """
