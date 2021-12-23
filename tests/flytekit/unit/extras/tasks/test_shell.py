import datetime
import os
import tempfile
from dataclasses import dataclass
from subprocess import CalledProcessError

import pytest
from dataclasses_json import dataclass_json

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


@pytest.mark.parametrize(
    "template_style,script",
    [
        (
            "default",
            """
            set -ex
            cat {{ .inputs.f }}
            echo "Hello World {{ .inputs.y }} on  {{ .inputs.j }}"
            """,
        ),
        (
            "python_f_string",
            """
            set -ex
            cat {f}
            echo "Hello World {y} on  {j}"
            """,
        ),
    ],
)
def test_input_substitution_primitive(template_style, script):
    t = ShellTask(
        name="test",
        script=script,
        template_style=template_style,
        inputs=kwtypes(f=str, y=int, j=datetime.datetime),
    )

    t(f=os.path.join(test_file_path, "__init__.py"), y=5, j=datetime.datetime(2021, 11, 10, 12, 15, 0))
    t(f=os.path.join(test_file_path, "test_shell.py"), y=5, j=datetime.datetime(2021, 11, 10, 12, 15, 0))
    with pytest.raises(CalledProcessError):
        t(f="non_exist.py", y=5, j=datetime.datetime(2021, 11, 10, 12, 15, 0))


@pytest.mark.parametrize(
    "template_style,script",
    [
        (
            "default",
            """
            cat {{ .inputs.f }}
            echo "Hello World {{ .inputs.y }} on  {{ .inputs.j }}"
            """,
        ),
        (
            "python_f_string",
            """
            cat {f}
            echo "Hello World {y} on  {j}"
            """,
        ),
    ],
)
def test_input_substitution_files(template_style, script):
    t = ShellTask(
        name="test",
        script=script,
        inputs=kwtypes(f=CSVFile, y=FlyteDirectory, j=datetime.datetime),
    )

    assert t(f=test_csv, y=testdata, j=datetime.datetime(2021, 11, 10, 12, 15, 0)) is None


@pytest.mark.parametrize(
    "template_style,script,output_location",
    [
        ("default", """cat {{ .inputs.f }} > {{ .outputs.y }}""", "{{ .inputs.f }}.mod"),
        ("python_f_string", """cat {f} > {y}""", "{f}.mod"),
    ],
)
def test_input_output_substitution_files(template_style, script, output_location):
    t = ShellTask(
        name="test",
        debug=True,
        script=script,
        template_style=template_style,
        inputs=kwtypes(f=CSVFile),
        output_locs=[
            OutputLocation(var="y", var_type=FlyteFile, location=output_location),
        ],
    )

    assert t.script == script

    contents = "1,2,3,4\n"
    with tempfile.TemporaryDirectory() as tmp:
        csv = os.path.join(tmp, "abc.csv")
        print(csv)
        with open(csv, "w") as f:
            f.write(contents)
        y = t(f=csv)
        assert y.path[-4:] == ".mod"
        assert os.path.exists(y.path)
        with open(y.path) as f:
            s = f.read()
        assert s == contents


@pytest.mark.parametrize(
    "template_style,script,output_location",
    [
        (
            "default",
            """
            cat {{ .inputs.f }} >> {{ .outputs.z }}
            echo "Hello World {{ .inputs.y }} on  {{ .inputs.j }}"
            """,
            "{{ .inputs.f }}.pyc",
        ),
        (
            "python_f_string",
            """
            cat {f} >> {z}
            echo "Hello World {y} on  {j}"
            """,
            "{f}.pyc",
        ),
    ],
)
def test_input_single_output_substitution_files(template_style, script, output_location):
    t = ShellTask(
        name="test",
        debug=True,
        script=script,
        template_style=template_style,
        inputs=kwtypes(f=CSVFile, y=FlyteDirectory, j=datetime.datetime),
        output_locs=[OutputLocation(var="z", var_type=FlyteFile, location=output_location)],
    )

    assert t.script == script
    y = t(f=test_csv, y=testdata, j=datetime.datetime(2021, 11, 10, 12, 15, 0))
    assert y.path[-4:] == ".pyc"


@pytest.mark.parametrize(
    "template_style,script,output_location_x,output_location_z",
    [
        (
            "default",
            """
            cat {{ .inputs.f }} {{ .inputs.missing }} >> {{ .outputs.z }}
            echo "Hello World {{ .inputs.y }} on  {{ .inputs.j }} - output {{.outputs.x}}"
            """,
            "{{ .inputs.y }}",
            "{{ .inputs.f }}.pyc",
        ),
        (
            "python_f_string",
            """
            cat {f} {missing} >> {z}
            echo "Hello World {y} on  {j} - output {x}"
            """,
            "{y}",
            "{f}.pyc",
        ),
    ],
)
def test_input_output_extra_var_in_template(template_style, script, output_location_x, output_location_z):
    t = ShellTask(
        name="test",
        debug=True,
        script=script,
        template_style=template_style,
        inputs=kwtypes(f=CSVFile, y=FlyteDirectory, j=datetime.datetime),
        output_locs=[
            OutputLocation(var="x", var_type=FlyteDirectory, location=output_location_x),
            OutputLocation(var="z", var_type=FlyteFile, location=output_location_z),
        ],
    )

    with pytest.raises(ValueError, match="missing"):
        t(f=test_csv, y=testdata, j=datetime.datetime(2021, 11, 10, 12, 15, 0))


@pytest.mark.parametrize(
    "template_style,script,output_location_x,output_location_z",
    [
        (
            "default",
            """
            cat {{ .inputs.missing }} >> {{ .outputs.y }}
            echo "Hello World {{ .inputs.y }} on  {{ .inputs.j }} - output {{.outputs.x}}"
            """,
            "{{ .inputs.y }}",
            "{{ .inputs.f }}.pyc",
        ),
        (
            "python_f_string",
            """
            cat {missing} >> {z}
            echo "Hello World {y} on  {j} - output {x}"
            """,
            "{y}",
            "{f}.pyc",
        ),
    ],
)
def test_input_output_extra_input(template_style, script, output_location_x, output_location_z):
    t = ShellTask(
        name="test",
        debug=True,
        script=script,
        template_style=template_style,
        inputs=kwtypes(f=CSVFile, y=FlyteDirectory, j=datetime.datetime),
        output_locs=[
            OutputLocation(var="x", var_type=FlyteDirectory, location=output_location_x),
            OutputLocation(var="z", var_type=FlyteFile, location=output_location_z),
        ],
    )

    with pytest.raises(ValueError, match="missing"):
        t(f=test_csv, y=testdata, j=datetime.datetime(2021, 11, 10, 12, 15, 0))


def test_can_use_complex_types_for_inputs_to_f_string_template():
    @dataclass_json
    @dataclass
    class InputArgs:
        in_file: CSVFile

    t = ShellTask(
        name="test",
        debug=True,
        script="""cat {input_args.in_file} >> {input_args.in_file}.tmp""",
        template_style="python_f_string",
        inputs=kwtypes(input_args=InputArgs),
        output_locs=[
            OutputLocation(var="x", var_type=FlyteFile, location="{input_args.in_file}.tmp"),
        ],
    )

    input_args = InputArgs(FlyteFile(path=test_csv))
    x = t(input_args=input_args)
    assert x.path[-4:] == ".tmp"


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
