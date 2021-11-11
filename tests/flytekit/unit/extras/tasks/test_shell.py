import datetime

import pytest

from flytekit import kwtypes
from flytekit.extras.tasks.shell import ShellTask, OutputLocation
from flytekit.types.directory import FlyteDirectory
from flytekit.types.file import CSVFile, FlyteFile


def test_shell_task_no_io():
    t = ShellTask(
        name="test",
        script="""
        echo "Hello World!"
        """,
    )

    t()


def test_shell_task_fail():
    t = ShellTask(
        name="test",
        script="""
        non-existent blah
        """,
    )

    with pytest.raises(Exception):
        t()


def test_input_substitution_primitive():
    t = ShellTask(
        name="test",
        script="""
        cat {{ .inputs.f }}
        echo "Hello World {{ .inputs.y }} on  {{ .inputs.j }}"
        """,
        inputs=kwtypes(f=str, y=int, j=datetime.datetime),
    )

    t(f="__init__.py", y=5, j=datetime.datetime(2021, 11, 10, 12, 15, 0))
    t(f="test_shell.py",  y=5, j=datetime.datetime(2021, 11, 10, 12, 15, 0))
    with pytest.raises(Exception):
        t(f="non_exist.py",  y=5, j=datetime.datetime(2021, 11, 10, 12, 15, 0))


def test_input_substitution_files():
    t = ShellTask(
        name="test",
        script="""
        cat {{ .inputs.f }}
        echo "Hello World {{ .inputs.y }} on  {{ .inputs.j }}"
        """,
        inputs=kwtypes(f=CSVFile, y=FlyteDirectory, j=datetime.datetime),
    )

    t(f="testdata/test.csv",  y="testdata", j=datetime.datetime(2021, 11, 10, 12, 15, 0))


def test_input_output_substitution_files():
    t = ShellTask(
        name="test",
        debug=True,
        script="""
        cat {{ .inputs.f }} >> {{ .outputs.y }}
        echo "Hello World {{ .inputs.y }} on  {{ .inputs.j }} - output {{.outputs.x}}"
        """,
        inputs=kwtypes(f=CSVFile, y=FlyteDirectory, j=datetime.datetime),
        output_locs=[OutputLocation(var="x", var_type=FlyteDirectory, location="{{ .inputs.y }}"),
                     OutputLocation(var="y", var_type=FlyteFile, location="{{ .inputs.f }}.pyc")]
    )

    t(f="testdata/test.csv",  y="testdata", j=datetime.datetime(2021, 11, 10, 12, 15, 0))