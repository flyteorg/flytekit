from __future__ import absolute_import

import argparse
import logging
import subprocess
from os import environ

FLYTE_ARG_PREFIX = "--__FLYTE"
FLYTE_ENV_VAR_PREFIX = f"{FLYTE_ARG_PREFIX}_ENV_VAR_"
FLYTE_CMD_PREFIX = f"{FLYTE_ARG_PREFIX}_CMD_"
FLYTE_ARG_SUFFIX = "__"

parser = argparse.ArgumentParser(description="Running sagemaker task")
args, unknowns = parser.parse_known_args()

# Parse the command line and env vars
flyte_cmd = []
env_vars = {}

for unknown in unknowns:
    logging.info(f'Processing argument {unknown}')
    if unknown.startswith(FLYTE_CMD_PREFIX) and unknown.endswith(FLYTE_ARG_SUFFIX):
        processed = unknown[len(FLYTE_CMD_PREFIX):][: -len(FLYTE_ARG_SUFFIX)]
        flyte_cmd.append(processed)
    elif unknown.startswith(FLYTE_ENV_VAR_PREFIX) and unknown.endswith(FLYTE_ARG_SUFFIX):
        processed = unknown[len(FLYTE_ENV_VAR_PREFIX):][: -len(FLYTE_ARG_SUFFIX)].split('=', maxsplit=2)
        env_vars[processed[0]] = processed[1]

logging.info(f"Cmd:{flyte_cmd}")
logging.info(f"Env vars:{env_vars}")

for key, val in env_vars:
    environ[key] = val

# Launching a subprocess with the selected entrypoint script and the rest of the arguments
logging.info(f"Launching command: {flyte_cmd}")
subprocess.run(flyte_cmd)
