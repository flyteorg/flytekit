import os
import shutil

import pytest
from click.testing import CliRunner
from flyteidl.admin.launch_plan_pb2 import LaunchPlan
from flyteidl.admin.task_pb2 import TaskSpec
from flyteidl.admin.workflow_pb2 import WorkflowSpec

import flytekit
import flytekit.configuration
import flytekit.tools.serialize_helpers
from flytekit.clis.sdk_in_container import pyflyte
from flytekit.core import context_manager
from flytekit.exceptions.user import FlyteValidationException

sample_file_contents = """
from flytekit import task, workflow

@task(cache=True, cache_version="1", retries=3)
def sum(x: int, y: int) -> int:
    return x + y

@task(cache=True, cache_version="1", retries=3)
def square(z: int) -> int:
    return z*z

@workflow
def my_workflow(x: int, y: int) -> int:
    return sum(x=square(z=x), y=square(z=y))
"""


@flytekit.task
def foo():
    pass


@flytekit.workflow
def wf():
    return foo()


def test_get_registrable_entities():
    ctx = context_manager.FlyteContextManager.current_context().with_serialization_settings(
        flytekit.configuration.SerializationSettings(
            project="p",
            domain="d",
            version="v",
            image_config=flytekit.configuration.ImageConfig(
                default_image=flytekit.configuration.Image("def", "docker.io/def", "latest")
            ),
        )
    )
    context_manager.FlyteEntities.entities = [foo, wf, "str"]
    entities = flytekit.tools.serialize_helpers.get_registrable_entities(ctx)
    assert entities
    assert len(entities) == 3

    for e in entities:
        if isinstance(e, WorkflowSpec) or isinstance(e, TaskSpec) or isinstance(e, LaunchPlan):
            continue
        assert False, f"found unknown entity {type(e)}"


def test_package_with_fast_registration():
    runner = CliRunner()
    with runner.isolated_filesystem():
        os.makedirs("core", exist_ok=True)
        with open(os.path.join("core", "sample.py"), "w") as f:
            f.write(sample_file_contents)
            f.close()
        result = runner.invoke(pyflyte.main, ["--pkgs", "core", "package", "--image", "core:v1", "--fast"])
        assert result.exit_code == 0
        assert "Successfully serialized" in result.output
        assert "Successfully packaged" in result.output
        result = runner.invoke(pyflyte.main, ["--pkgs", "core", "package", "--image", "core:v1", "--fast"])
        assert result.exit_code == 2
        assert "flyte-package.tgz already exists, specify -f to override" in result.output
        result = runner.invoke(
            pyflyte.main,
            ["--pkgs", "core", "package", "--image", "core:v1", "--fast", "--force"],
        )
        assert result.exit_code == 0
        assert "deleting and re-creating it" in result.output
        shutil.rmtree("core")


def test_duplicate_registrable_entities():
    @flytekit.task
    def t_1():
        pass

    # Keep a reference to a task named `t_1` that's going to be duplicated below
    reference_1 = t_1

    @flytekit.workflow
    def wf_1():
        return t_1()

    # Duplicate definition of `t_1`
    @flytekit.task
    def t_1() -> str:
        pass

    # Keep a second reference to the duplicate task named `t_1` so that we can use it later
    reference_2 = t_1

    @flytekit.task
    def non_duplicate_task():
        pass

    @flytekit.workflow
    def wf_2():
        non_duplicate_task()
        # refers to the second definition of `t_1`
        return t_1()

    ctx = context_manager.FlyteContextManager.current_context().with_serialization_settings(
        flytekit.configuration.SerializationSettings(
            project="p",
            domain="d",
            version="v",
            image_config=flytekit.configuration.ImageConfig(
                default_image=flytekit.configuration.Image("def", "docker.io/def", "latest")
            ),
        )
    )

    context_manager.FlyteEntities.entities = [reference_1, wf_1, "str", reference_2, non_duplicate_task, wf_2, "str"]

    with pytest.raises(
        FlyteValidationException,
        match=r"Multiple definitions of the following tasks were found: \['pyflyte.test_package.t_1'\]",
    ):
        flytekit.tools.serialize_helpers.get_registrable_entities(ctx)


def test_package():
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(
            pyflyte.main,
            [
                "--pkgs",
                "flytekit.unit.cli.pyflyte.test_package",
                "package",
                "--image",
                "myapp:03eccc1cf101adbd8c4734dba865d3fdeb720aa7",
            ],
        )
        assert result.exit_code == 1
        assert result.output is not None


def test_pkgs():
    pp = pyflyte.validate_package(None, None, ["a.b", "a.c,b.a", "cc.a"])
    assert pp == ["a.b", "a.c", "b.a", "cc.a"]


def test_package_with_no_pkgs():
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(pyflyte.main, ["package"])
        assert result.exit_code == 1
        assert "No packages to scan for flyte entities. Aborting!" in result.output
