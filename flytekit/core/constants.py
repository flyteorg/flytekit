INPUT_FILE_NAME = "inputs.pb"
OUTPUT_FILE_NAME = "outputs.pb"
FUTURES_FILE_NAME = "futures.pb"
ERROR_FILE_NAME = "error.pb"
REQUIREMENTS_FILE_NAME = "requirements.txt"
SOURCE_CODE = "source_code"

CONTAINER_ARRAY_TASK = "container_array"
GLOBAL_INPUT_NODE_ID = ""

START_NODE_ID = "start-node"
END_NODE_ID = "end-node"

# If set this environment variable overrides the default container image and the default base image in ImageSpec.
FLYTE_INTERNAL_IMAGE_ENV_VAR = "FLYTE_INTERNAL_IMAGE"

# Binary IDL Serialization Format
MESSAGEPACK = "msgpack"

# Set this environment variable to true to force the task to return non-zero exit code on failure.
FLYTE_FAIL_ON_ERROR = "FLYTE_FAIL_ON_ERROR"
