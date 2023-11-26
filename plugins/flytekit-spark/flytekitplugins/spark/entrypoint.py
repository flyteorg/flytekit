"""
This file is used to execute a Spark job on Databricks platform. It's used to override the entrypoint of spark-submit, in
order to download the input proto before running spark job and upload the output proto after execution.
"""
import os
import sys
from typing import List

import click
from flytekit.bin.entrypoint import fast_execute_task_cmd as _fast_execute_task_cmd
from flytekit.bin.entrypoint import execute_task_cmd as _execute_task_cmd
from flytekit.exceptions.user import FlyteUserException
from flytekit.tools.fast_registration import download_distribution


def fast_execute_task_cmd(additional_distribution: str, dest_dir: str, task_execute_cmd: List[str]):
    if additional_distribution is not None:
        if not dest_dir:
            dest_dir = os.getcwd()
        download_distribution(additional_distribution, dest_dir)
        os.chdir(dest_dir)

    # Insert the call to fast before the unbounded resolver args
    cmd = []
    for arg in task_execute_cmd:
        if arg == "--resolver":
            cmd.extend(["--dynamic-addl-distro", additional_distribution, "--dynamic-dest-dir", dest_dir])
        cmd.append(arg)

    click_ctx = click.Context(click.Command("dummy"))
    parser = _execute_task_cmd.make_parser(click_ctx)
    args, _, _ = parser.parse_args(cmd[1:])
    _execute_task_cmd.callback(test=False, **args)


def main():
    args = sys.argv
    click_ctx = click.Context(click.Command("dummy"))
    if args[1] == "pyflyte-fast-execute":
        parser = _fast_execute_task_cmd.make_parser(click_ctx)
        args, _, _ = parser.parse_args(args[2:])
        fast_execute_task_cmd(**args)
    elif args[1] == "pyflyte-execute":
        parser = _execute_task_cmd.make_parser(click_ctx)
        args, _, _ = parser.parse_args(args[2:])
        _execute_task_cmd.callback(test=False, dynamic_addl_distro=None, dynamic_dest_dir=None, **args)
    else:
        raise FlyteUserException(f"Unrecognized command: {args[1:]}")


if __name__ == "__main__":
    main()
