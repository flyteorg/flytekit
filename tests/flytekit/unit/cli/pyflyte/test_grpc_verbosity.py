import os


def test_grpc_verbosity_set_on_import():
    """
    Test that GRPC_VERBOSITY is set to NONE if not already present in the environment
    when the SDK container module is imported.
    """
    original_value = os.environ.get("GRPC_VERBOSITY", None)

    try:
        if "GRPC_VERBOSITY" in os.environ:
            del os.environ["GRPC_VERBOSITY"]

        import importlib
        import flytekit.clis.sdk_in_container
        importlib.reload(flytekit.clis.sdk_in_container)

        assert "GRPC_VERBOSITY" in os.environ
        assert os.environ["GRPC_VERBOSITY"] == "NONE"

    finally:
        if original_value is not None:
            os.environ["GRPC_VERBOSITY"] = original_value
        elif "GRPC_VERBOSITY" in os.environ:
            del os.environ["GRPC_VERBOSITY"]


def test_grpc_verbosity_not_overridden():
    """
    Test that GRPC_VERBOSITY is not overridden if already set in the environment.
    """
    original_value = os.environ.get("GRPC_VERBOSITY", None)

    try:
        os.environ["GRPC_VERBOSITY"] = "INFO"

        import importlib
        import flytekit.clis.sdk_in_container
        importlib.reload(flytekit.clis.sdk_in_container)

        assert os.environ["GRPC_VERBOSITY"] == "INFO"

    finally:
        if original_value is not None:
            os.environ["GRPC_VERBOSITY"] = original_value
        elif "GRPC_VERBOSITY" in os.environ:
            del os.environ["GRPC_VERBOSITY"]
