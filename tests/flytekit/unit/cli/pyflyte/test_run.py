import os

from click.testing import CliRunner

from flytekit.clis.sdk_in_container import pyflyte
from flytekit.clis.sdk_in_container.run import get_entities_in_file

WORKFLOW_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)), "workflow.py")


def test_pyflyte_run_wf():
    runner = CliRunner()
    module_path = WORKFLOW_FILE
    result = runner.invoke(pyflyte.main, ["run", module_path, "my_wf", "--help"], catch_exceptions=False)

    assert result.exit_code == 0


def test_pyflyte_run_cli():
    runner = CliRunner()
    dir_name = os.path.dirname(os.path.realpath(__file__))
    result = runner.invoke(
        pyflyte.main,
        [
            "run",
            os.path.join(dir_name, "workflow.py"),
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
            os.path.join(dir_name, "testdata/df.parquet"),
            "--i",
            "2020-05-01",
            "--j",
            "20H",
            "--k",
            "RED",
            "--remote",
            os.path.join(dir_name, "testdata"),
            "--image",
            os.path.join(dir_name, "testdata"),
            "--h",
        ],
        catch_exceptions=False,
    )
    print(result.stdout)
    assert result.exit_code == 0


def test_get_entities_in_file():
    e = get_entities_in_file(WORKFLOW_FILE)
    assert e.workflows == ["my_wf"]
    assert e.tasks == ["get_subset_df", "print_all", "show_sd"]
    assert e.all() == ["my_wf", "get_subset_df", "print_all", "show_sd"]
