from __future__ import absolute_import

import argparse
import logging
import subprocess
from os import environ

FLYTE_ARG_PREFIX = "--__FLYTE"
FLYTE_ENV_VAR_PREFIX = "{}_ENV_VAR_".format(FLYTE_ARG_PREFIX)
FLYTE_CMD_ARG_PREFIX = "{}_CMD_ARG_".format(FLYTE_ARG_PREFIX)
FLYTE_ARG_SUFFIX = "__"

parser = argparse.ArgumentParser(description="Running sagemaker task")
parser.add_argument("--__FLYTE_SAGEMAKER_CMD__", dest="flyte_sagemaker_cmd", help="The entrypoint selector argument")
args, unknowns = parser.parse_known_args()

# Extending the command with the rest of the command-line arguments
subprocess_cmd = []
if args.flyte_sagemaker_cmd is not None:
    subprocess_cmd = args.flyte_sagemaker_cmd.split("+")

pass_through_cmd_args = []
env_vars = []
i = 0

while i < len(unknowns):
    print(unknowns[i])
    if unknowns[i].startswith(FLYTE_CMD_ARG_PREFIX) and unknowns[i].endswith(FLYTE_ARG_SUFFIX):
        processed = unknowns[i][len(FLYTE_CMD_ARG_PREFIX):][:-len(FLYTE_ARG_SUFFIX)].replace("_", "-")
        pass_through_cmd_args.append("--" + processed)
        i += 1
        if unknowns[i].startswith(FLYTE_ARG_PREFIX) is False:
            pass_through_cmd_args.append(unknowns[i])
            i += 1
    elif unknowns[i].startswith(FLYTE_ENV_VAR_PREFIX) and unknowns[i].endswith(FLYTE_ARG_SUFFIX):
        processed = unknowns[i][len(FLYTE_ENV_VAR_PREFIX):][:-len(FLYTE_ARG_SUFFIX)]
        # Note that in env var we must not replace _ with -
        env_vars.append(processed)
        i += 1
        if unknowns[i].startswith(FLYTE_ARG_PREFIX) is False:
            env_vars.append(unknowns[i])
            i += 1
    else:
        i += 1

print("Pass-through cmd args:", pass_through_cmd_args)
print("Env vars:", env_vars)

for i in range(0, len(env_vars), 2):
    environ[env_vars[i]] = env_vars[i+1]

logging.info("Launching a subprocess with: {}".format(pass_through_cmd_args))

# Launching a subprocess with the selected entrypoint script and the rest of the arguments
# subprocess_cmd = []
subprocess_cmd.extend(pass_through_cmd_args)
print("Running a subprocess with the following command: {}".format(subprocess_cmd))
subprocess.run(subprocess_cmd)
