import os

# Set GRPC_VERBOSITY to NONE if not already set to silence unwanted output
# This addresses the issue with grpcio >=1.68.0 causing unwanted output
# https://github.com/flyteorg/flyte/issues/6082
if "GRPC_VERBOSITY" not in os.environ:
    os.environ["GRPC_VERBOSITY"] = "NONE"
