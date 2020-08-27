from __future__ import absolute_import

import argparse
from os import environ

from flytekit.bin.entrypoint import execute_task_cmd

parser = argparse.ArgumentParser(description="Running sagemaker task")
parser.add_argument('--__FLYTE_CMD__', dest='flyte_cmd', type=str, nargs='+',
                    help='The entrypoint selector argument')
parser.add_argument('--__FLYTE_ENV_VAR__', dest='flyte_env_var', type=str, nargs='+',
                    help='Specifies an environment variable.')
args, unknowns = parser.parse_known_args()

# Defer to click to parse the command
ctx = execute_task_cmd.make_context(args.flyte_sagmaker_cmd[1], args=args.flyte_sagmaker_cmd[2:])
execute_task_cmd.parse_args(ctx=ctx, args=args.flyte_sagmaker_cmd[2:])

# Rewrite output_prefix by appending the training job name. SageMaker populates TRAINING_JOB_NAME env var.
# The real value in doing this shows in HPO jobs. In HPO case, SageMaker runs the same container with pretty
# much the same arguments with the exception of TRAINING_JOB_NAME and the hyper parameter it's optimizing.
# Since, at least for now, flytekit will continue to write its outputs.pb, we need to make sure the target
# output directory is unique hence why we use the TRAINING_JOB_NAME as a subdirectory.
ctx.params["output_prefix"] = "{}/{}".format(ctx.params["output_prefix"], environ.get("TRAINING_JOB_NAME"))

# Set environment variables passed in through command line argument.
for env_var in args.flyte_env_var:
    env_var_parts = env_var.split('=', maxsplit=1)
    environ[env_var_parts[0]] = env_var_parts[1]

# Finally, invoke the command with the updated params.
res = execute_task_cmd.invoke(ctx)
