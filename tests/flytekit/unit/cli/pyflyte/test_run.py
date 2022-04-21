from click.testing import CliRunner

from flytekit.clis.sdk_in_container import pyflyte


def test_pyflyte_run_cli():
    runner = CliRunner()
    result = runner.invoke(
        pyflyte.main,
        [
            "run",
            "workflow.py",
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
            "testdata/df.parquet",
            "--i",
            "2020-05-01",
            "--j",
            "20H",
            "--k",
            "RED",
            "--remote",
            "testdata",
            "--image",
            "testdata",
            "--h",
        ],
        catch_exceptions=False,
    )
    print(result.stdout)
    assert result.exit_code == 0
