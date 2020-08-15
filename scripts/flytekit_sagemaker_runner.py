from __future__ import absolute_import
import argparse
import logging
import subprocess

parser = argparse.ArgumentParser(description="Running sagemaker task")
parser.add_argument('--__FLYTE_SAGEMAKER_CMD__', dest='flyte_sagmaker_cmd',
                    help='The entrypoint selector argument')
args, unknowns = parser.parse_known_args()

# Extending the command with the rest of the command-line arguments
subprocess_cmd = args.flyte_sagmaker_cmd.split()
logging.info("Launching a subprocess with: {}".format(subprocess_cmd))

# Launching a subprocess with the selected entrypoint script and the rest of the arguments
subprocess.run(subprocess_cmd)
