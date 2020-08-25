from __future__ import absolute_import

import argparse
import logging
import subprocess
from os import environ

FLYTE_ARGS_PREFIX = "--__FLYTE_"
FLYTE_ARGS_SUFFIX = "__"

parser = argparse.ArgumentParser(description="Running sagemaker task")
parser.add_argument("--__FLYTE_SAGEMAKER_CMD__", dest="flyte_sagemaker_cmd", help="The entrypoint selector argument")
args, unknowns = parser.parse_known_args()

# Extending the command with the rest of the command-line arguments
subprocess_cmd = []
if args.flyte_sagemaker_cmd is not None:
    subprocess_cmd = args.flyte_sagemaker_cmd.split("+")

for i in range(len(unknowns)):
    if unknowns[i].startswith(FLYTE_ARGS_PREFIX) and unknowns[i].endswith(FLYTE_ARGS_SUFFIX):
        unknowns[i] = unknowns[i][len(FLYTE_ARGS_PREFIX) :]
        unknowns[i] = unknowns[i].replace("_", "-")
        unknowns[i] = "--" + unknowns[i][: -len(FLYTE_ARGS_SUFFIX)]

flyte_sagemaker_cmd_parser = argparse.ArgumentParser(
    description="Parse pyflyte execute command to replace output prefix location."
)

if args.flyte_sagemaker_cmd is not None:
    flyte_sagemaker_cmd_parser.add_argument("--output-prefix", dest="output_prefix", required=True)
    args, unknowns = flyte_sagemaker_cmd_parser.parse_known_args(args=unknowns)
    args.output_prefix = "{}/{}".format(args.output_prefix, environ.get("TRAINING_JOB_NAME", ""))
    unknowns.extend(["--output-prefix", args.output_prefix])

logging.info("Launching a subprocess with: {}".format(unknowns))

# Launching a subprocess with the selected entrypoint script and the rest of the arguments
# subprocess_cmd = []
subprocess_cmd.extend(unknowns)
subprocess.run(subprocess_cmd)
