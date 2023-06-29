INPUT_FILE_NAME = "inputs.pb"
OUTPUT_FILE_NAME = "outputs.pb"
FUTURES_FILE_NAME = "futures.pb"
ERROR_FILE_NAME = "error.pb"

CONTAINER_ARRAY_TASK = "container_array"

GLOBAL_INPUT_NODE_ID = ""

START_NODE_ID = "start-node"
END_NODE_ID = "end-node"


class CloudProvider(object):
    AWS = "aws"
    GCP = "gcp"
    LOCAL = "local"
