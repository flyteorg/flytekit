import os
import sys

from click.testing import CliRunner

from flytekit.clis.sdk_in_container import pyflyte

sample_file_contents = """
from flytekit import task
from flytekit.image_spec.image_spec import ImageSpec

@task()
def t1() -> str:
    return "hello"
"""


def test_build():
    runner = CliRunner()
    with runner.isolated_filesystem():
        os.makedirs("core", exist_ok=True)
        with open(os.path.join("core", "sample.py"), "w") as f:
            f.write(sample_file_contents)
        sys.path.append(os.path.join(os.getcwd(), "core"))
        result = runner.invoke(pyflyte.main, ["build", "--file", "core/sample.py"])
        assert result.output == ""
        assert result.exit_code == 0
