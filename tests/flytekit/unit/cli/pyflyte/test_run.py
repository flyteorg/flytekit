import os
import pathlib

import pytest
from click.testing import CliRunner

from flytekit.clis.sdk_in_container import pyflyte
from flytekit.clis.sdk_in_container.run import get_entities_in_file

WORKFLOW_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)), "workflow.py")
DIR_NAME = os.path.dirname(os.path.realpath(__file__))


def test_pyflyte_run_wf():
    runner = CliRunner()
    module_path = WORKFLOW_FILE
    result = runner.invoke(pyflyte.main, ["run", module_path, "my_wf", "--help"], catch_exceptions=False)

    assert result.exit_code == 0


def test_pyflyte_run_cli():
    runner = CliRunner()
    result = runner.invoke(
        pyflyte.main,
        [
            "run",
            WORKFLOW_FILE,
            "my_wf",
            "--a",
            "1",
            "--b",
            "Hello",
            "--c",
            "1.1",
            "--d",
            '{"i":1,"a":["h","e"]}',
            "--e",
            "[1,2,3]",
            "--f",
            '{"x":1.0, "y":2.0}',
            "--g",
            os.path.join(DIR_NAME, "testdata/df.parquet"),
            "--i",
            "2020-05-01",
            "--j",
            "20H",
            "--k",
            "RED",
            "--remote",
            os.path.join(DIR_NAME, "testdata"),
            "--image",
            os.path.join(DIR_NAME, "testdata"),
            "--h",
        ],
        catch_exceptions=False,
    )
    print(result.stdout)
    assert result.exit_code == 0


@pytest.mark.parametrize(
    "input",
    ["1", os.path.join(DIR_NAME, "testdata/df.parquet"), "[1,2,3]", '{"i":1,"a":["h","e"]}', "2020-05-01", "RED"],
)
def test_union_type(input):
    runner = CliRunner()
    dir_name = os.path.dirname(os.path.realpath(__file__))
    result = runner.invoke(
        pyflyte.main,
        [
            "run",
            os.path.join(dir_name, "workflow.py"),
            "test_union",
            "--a",
            input,
        ],
        catch_exceptions=False,
    )
    print(result.stdout)
    assert result.exit_code == 0


def test_get_entities_in_file():
    e = get_entities_in_file(WORKFLOW_FILE)
    assert e.workflows == ["my_wf"]
    assert e.tasks == ["get_subset_df", "print_all", "show_sd", "test_union"]
    assert e.all() == ["my_wf", "get_subset_df", "print_all", "show_sd", "test_union"]


@pytest.mark.parametrize(
    "working_dir, wf_path",
    [
        (pathlib.Path("test_nested_wf"), os.path.join("a", "b", "c", "d", "wf.py")),
        (pathlib.Path("test_nested_wf", "a"), os.path.join("b", "c", "d", "wf.py")),
        (pathlib.Path("test_nested_wf", "a", "b"), os.path.join("c", "d", "wf.py")),
        (pathlib.Path("test_nested_wf", "a", "b", "c"), os.path.join("d", "wf.py")),
        (pathlib.Path("test_nested_wf", "a", "b", "c", "d"), os.path.join("wf.py")),
    ],
)
def test_nested_workflow(working_dir, wf_path, monkeypatch: pytest.MonkeyPatch):
    runner = CliRunner()
    base_path = os.path.dirname(os.path.realpath(__file__))
    # Change working directory without side-effects (i.e. just for this test)
    monkeypatch.chdir(os.path.join(base_path, working_dir))
    result = runner.invoke(
        pyflyte.main,
        [
            "run",
            wf_path,
            "wf_id",
            "--m",
            "wow",
        ],
        catch_exceptions=False,
    )
    assert result.stdout.strip() == "wow"
    assert result.exit_code == 0


@pytest.mark.parametrize(
    "wf_path",
    [("collection_wf.py"), ("map_wf.py"), ("dataclass_wf.py")],
)
def test_list_default_arguments(wf_path):
    runner = CliRunner()
    dir_name = os.path.dirname(os.path.realpath(__file__))
    result = runner.invoke(
        pyflyte.main,
        [
            "run",
            os.path.join(dir_name, "default_arguments", wf_path),
            "wf",
        ],
        catch_exceptions=False,
    )
    print(result.stdout)
    assert result.exit_code == 0
