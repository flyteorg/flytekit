from __future__ import absolute_import
from flytekit.bin import entrypoint as _entrypoint
import click as _click


@_click.group()
def sparkrunner():
    pass


@sparkrunner.command()
@_click.option('--task-module', required=True)
@_click.option('--task-name', required=True)
@_click.option('--inputs', required=True)
@_click.option('--output-prefix', required=True)
@_click.option('--test', is_flag=True)
def execute_spark_task(task_module, task_name, inputs, output_prefix, test):
    _entrypoint.execute_task(task_module, task_name, inputs, output_prefix, test)


if __name__ == "__main__":
    sparkrunner()
