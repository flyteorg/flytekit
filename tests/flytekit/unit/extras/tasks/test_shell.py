import datetime
import os
from subprocess import CalledProcessError

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
        set -ex
        cat {{ .inputs.f }}
        echo "Hello World {{ .inputs.y }} on  {{ .inputs.j }}"
        """,
        inputs=kwtypes(f=str, y=int, j=datetime.datetime),
    )

    t(f="__init__.py", y=5, j=datetime.datetime(2021, 11, 10, 12, 15, 0))
    t(f="test_shell.py", y=5, j=datetime.datetime(2021, 11, 10, 12, 15, 0))
    with pytest.raises(CalledProcessError):
        t(f="non_exist.py", y=5, j=datetime.datetime(2021, 11, 10, 12, 15, 0))


def test_input_substitution_files():
    t = ShellTask(
        name="test",
        script="""
        cat {{ .inputs.f }}
        echo "Hello World {{ .inputs.y }} on  {{ .inputs.j }}"
        """,
        inputs=kwtypes(f=CSVFile, y=FlyteDirectory, j=datetime.datetime),
    )

    t(f="testdata/test.csv", y="testdata", j=datetime.datetime(2021, 11, 10, 12, 15, 0))


def test_input_output_substitution_files():
    s = """
        cat {{ .inputs.f }} >> {{ .outputs.y }}
        echo "Hello World {{ .inputs.y }} on  {{ .inputs.j }} - output {{.outputs.x}}"
        """
    t = ShellTask(
        name="test",
        debug=True,
        script=s,
        inputs=kwtypes(f=CSVFile, y=FlyteDirectory, j=datetime.datetime),
        output_locs=[OutputLocation(var="x", var_type=FlyteDirectory, location="{{ .inputs.y }}"),
                     OutputLocation(var="y", var_type=FlyteFile, location="{{ .inputs.f }}.pyc")]
    )

    assert t.script == s
    t(f="testdata/test.csv", y="testdata", j=datetime.datetime(2021, 11, 10, 12, 15, 0))


def test_input_output_missing_var():
    t = ShellTask(
        name="test",
        debug=True,
        script="""
        cat {{ .inputs.f }} {{ .inputs.missing }} >> {{ .outputs.y }}
        echo "Hello World {{ .inputs.y }} on  {{ .inputs.j }} - output {{.outputs.x}}"
        """,
        inputs=kwtypes(f=CSVFile, y=FlyteDirectory, j=datetime.datetime),
        output_locs=[OutputLocation(var="x", var_type=FlyteDirectory, location="{{ .inputs.y }}"),
                     OutputLocation(var="y", var_type=FlyteFile, location="{{ .inputs.f }}.pyc")]
    )

    with pytest.raises(ValueError):
        t(f="testdata/test.csv", y="testdata", j=datetime.datetime(2021, 11, 10, 12, 15, 0))


def test_shell_script():
    t = ShellTask(
        name="test",
        debug=True,
        script_file="testdata/script.sh",
        inputs=kwtypes(f=CSVFile, y=FlyteDirectory, j=datetime.datetime),
        output_locs=[OutputLocation(var="x", var_type=FlyteDirectory, location="{{ .inputs.y }}"),
                     OutputLocation(var="y", var_type=FlyteFile, location="{{ .inputs.f }}.pyc")]
    )

    assert t.script_file == os.path.abspath("testdata/script.sh")
    t(f="testdata/test.csv", y="testdata", j=datetime.datetime(2021, 11, 10, 12, 15, 0))
