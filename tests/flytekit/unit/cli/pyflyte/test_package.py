import click
import pytest
from click.testing import CliRunner
from flyteidl.admin.launch_plan_pb2 import LaunchPlan
from flyteidl.admin.task_pb2 import TaskSpec
from flyteidl.admin.workflow_pb2 import WorkflowSpec

import flytekit
from flytekit.clis.sdk_in_container import package, pyflyte, serialize
from flytekit.common.exceptions.user import FlyteValidationException
from flytekit.core import context_manager


def test_validate_image():
    ic = package.validate_image(None, "image", ())
    assert ic
    assert ic.default_image is None

    img1 = "xyz:latest"
    img2 = "docker.io/xyz:latest"
    img3 = "docker.io/xyz:latest"
    img3_cli = f"default={img3}"
    img4 = "docker.io/my:azb"
    img4_cli = f"my_img={img4}"

    ic = package.validate_image(None, "image", (img1,))
    assert ic
    assert ic.default_image.full == img1

    ic = package.validate_image(None, "image", (img2,))
    assert ic
    assert ic.default_image.full == img2

    ic = package.validate_image(None, "image", (img3_cli,))
    assert ic
    assert ic.default_image.full == img3

    with pytest.raises(click.BadParameter):
        package.validate_image(None, "image", (img1, img3_cli))

    with pytest.raises(click.BadParameter):
        package.validate_image(None, "image", (img1, img2))

    with pytest.raises(click.BadParameter):
        package.validate_image(None, "image", (img1, img1))

    ic = package.validate_image(None, "image", (img3_cli, img4_cli))
    assert ic
    assert ic.default_image.full == img3
    assert len(ic.images) == 1
    assert ic.images[0].full == img4


@flytekit.task
def foo():
    pass


@flytekit.workflow
def wf():
    return foo()


def test_get_registrable_entities():
    ctx = context_manager.FlyteContextManager.current_context().with_serialization_settings(
        context_manager.SerializationSettings(
            project="p",
            domain="d",
            version="v",
            image_config=context_manager.ImageConfig(
                default_image=context_manager.Image("def", "docker.io/def", "latest")
            ),
        )
    )
    context_manager.FlyteEntities.entities = [foo, wf, "str"]
    entities = serialize.get_registrable_entities(ctx)
    assert entities
    assert len(entities) == 3

    for e in entities:
        if isinstance(e, WorkflowSpec) or isinstance(e, TaskSpec) or isinstance(e, LaunchPlan):
            continue
        assert False, f"found unknown entity {type(e)}"


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
        context_manager.SerializationSettings(
            project="p",
            domain="d",
            version="v",
            image_config=context_manager.ImageConfig(
                default_image=context_manager.Image("def", "docker.io/def", "latest")
            ),
        )
    )

    context_manager.FlyteEntities.entities = [reference_1, wf_1, "str", reference_2, non_duplicate_task, wf_2, "str"]

    with pytest.raises(
        FlyteValidationException,
        match=r"Multiple definitions of the following tasks were found: \['pyflyte.test_package.t_1'\]",
    ):
        serialize.get_registrable_entities(ctx)


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
