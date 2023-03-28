from click.testing import CliRunner

from flytekit.clis.sdk_in_container import pyflyte

sample_file_contents = """
from flytekit import workflow
from flytekit.image_spec.image_spec import ImageSpec

@task(image_spec=ImageSpec(packages=["pandas", "numpy"], registry="pingsutw"))
def t1() -> str:
    return "hello"
"""


def test_build():
    runner = CliRunner()
    with runner.isolated_filesystem():
        os.makedirs("core", exist_ok=True)
        with open(os.path.join("core", "sample.py"), "w") as f:
            f.write(sample_file_contents)
        result = runner.invoke(pyflyte.main, ["build", "--file", "core/sample.py"])
        assert result.exit_code == 0
        # assert "Successfully serialized" in result.output
        # assert "Successfully packaged" in result.output
        # result = runner.invoke(pyflyte.main, ["--pkgs", "core", "package", "--image", "core:v1", "--fast"])
        # assert result.exit_code == 2
        # assert "flyte-package.tgz already exists, specify -f to override" in result.output
        # result = runner.invoke(
        #     pyflyte.main,
        #     ["--pkgs", "core", "package", "--image", "core:v1", "--fast", "--force"],
        # )
        # assert result.exit_code == 0
        # assert "deleting and re-creating it" in result.output
        # shutil.rmtree("core")