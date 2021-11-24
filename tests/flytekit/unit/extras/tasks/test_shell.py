import datetime
import os
from subprocess import CalledProcessError

import pytest

from flytekit import kwtypes
from flytekit.extras.tasks.shell import OutputLocation, ShellTask
from flytekit.types.directory import FlyteDirectory
from flytekit.types.file import CSVFile, FlyteFile

test_file_path = os.path.dirname(os.path.realpath(__file__))
testdata = os.path.join(test_file_path, "testdata")
script_sh = os.path.join(testdata, "script.sh")
test_csv = os.path.join(testdata, "test.csv")


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

    t(f=os.path.join(test_file_path, "__init__.py"), y=5, j=datetime.datetime(2021, 11, 10, 12, 15, 0))
    t(f=os.path.join(test_file_path, "test_shell.py"), y=5, j=datetime.datetime(2021, 11, 10, 12, 15, 0))
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

    assert t(f=test_csv, y=testdata, j=datetime.datetime(2021, 11, 10, 12, 15, 0)) is None


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
        output_locs=[
            OutputLocation(var="x", var_type=FlyteDirectory, location="{{ .inputs.y }}"),
            OutputLocation(var="y", var_type=FlyteFile, location="{{ .inputs.f }}.pyc"),
        ],
    )

    assert t.script == s
    x, y = t(f=test_csv, y=testdata, j=datetime.datetime(2021, 11, 10, 12, 15, 0))
    assert x is not None
    assert y.path[-4:] == ".pyc"


def test_input_single_output_substitution_files():
    s = """
        cat {{ .inputs.f }} >> {{ .outputs.y }}
        echo "Hello World {{ .inputs.y }} on  {{ .inputs.j }}"
        """
    t = ShellTask(
        name="test",
        debug=True,
        script=s,
        inputs=kwtypes(f=CSVFile, y=FlyteDirectory, j=datetime.datetime),
        output_locs=[OutputLocation(var="y", var_type=FlyteFile, location="{{ .inputs.f }}.pyc")],
    )

    assert t.script == s
    y = t(f=test_csv, y=testdata, j=datetime.datetime(2021, 11, 10, 12, 15, 0))
    assert y.path[-4:] == ".pyc"


def test_input_output_extra_var_in_template():
    t = ShellTask(
        name="test",
        debug=True,
        script="""
        cat {{ .inputs.f }} {{ .inputs.missing }} >> {{ .outputs.y }}
        echo "Hello World {{ .inputs.y }} on  {{ .inputs.j }} - output {{.outputs.x}}"
        """,
        inputs=kwtypes(f=CSVFile, y=FlyteDirectory, j=datetime.datetime),
        output_locs=[
            OutputLocation(var="x", var_type=FlyteDirectory, location="{{ .inputs.y }}"),
            OutputLocation(var="y", var_type=FlyteFile, location="{{ .inputs.f }}.pyc"),
        ],
    )

    with pytest.raises(ValueError):
        t(f=test_csv, y=testdata, j=datetime.datetime(2021, 11, 10, 12, 15, 0))


def test_input_output_extra_input():
    t = ShellTask(
        name="test",
        debug=True,
        script="""
        cat {{ .inputs.missing }} >> {{ .outputs.y }}
        echo "Hello World {{ .inputs.y }} on  {{ .inputs.j }} - output {{.outputs.x}}"
        """,
        inputs=kwtypes(f=CSVFile, y=FlyteDirectory, j=datetime.datetime),
        output_locs=[
            OutputLocation(var="x", var_type=FlyteDirectory, location="{{ .inputs.y }}"),
            OutputLocation(var="y", var_type=FlyteFile, location="{{ .inputs.f }}.pyc"),
        ],
    )

    with pytest.raises(ValueError):
        t(f=test_csv, y=testdata, j=datetime.datetime(2021, 11, 10, 12, 15, 0))


def test_shell_script():
    t = ShellTask(
        name="test2",
        debug=True,
        script_file=script_sh,
        inputs=kwtypes(f=CSVFile, y=FlyteDirectory, j=datetime.datetime),
        output_locs=[
            OutputLocation(var="x", var_type=FlyteDirectory, location="{{ .inputs.y }}"),
            OutputLocation(var="y", var_type=FlyteFile, location="{{ .inputs.f }}.pyc"),
        ],
    )

    assert t.script_file == script_sh
    t(f=test_csv, y=testdata, j=datetime.datetime(2021, 11, 10, 12, 15, 0))
