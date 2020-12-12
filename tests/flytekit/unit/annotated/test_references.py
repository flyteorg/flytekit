import typing

import pytest

from flytekit.annotated.reference_entity import TaskReference
from flytekit.annotated import context_manager
from flytekit.annotated.context_manager import Image, ImageConfig
from flytekit.annotated.task import task
from flytekit.annotated.testing import task_mock
from flytekit.annotated.workflow import workflow
from flytekit.annotated.reference import get_reference_entity
from flytekit.models.core import identifier as _identifier_model
from flytekit.annotated.base_task import kwtypes


def test_ref():
    @task(
        task_config=TaskReference(
            project="flytesnacks",
            domain="development",
            name="recipes.aaa.simple.join_strings",
            version="553018f39e519bdb2597b652639c30ce16b99c79",
        )
    )
    def ref_t1(a: typing.List[str]) -> str:
        ...

    assert ref_t1.id.project == "flytesnacks"
    assert ref_t1.id.domain == "development"
    assert ref_t1.id.name == "recipes.aaa.simple.join_strings"
    assert ref_t1.id.version == "553018f39e519bdb2597b652639c30ce16b99c79"

    registration_settings = context_manager.RegistrationSettings(
        project="proj",
        domain="dom",
        version="123",
        image_config=ImageConfig(Image(name="name", fqn="asdf/fdsa", tag="123")),
        env={},
        iam_role="test:iam:role",
        service_account=None,
    )
    with context_manager.FlyteContext.current_context().new_registration_settings(
        registration_settings=registration_settings
    ):
        sdk_task = ref_t1.get_registerable_entity()
        assert sdk_task.has_registered
        assert sdk_task.id.project == "flytesnacks"
        assert sdk_task.id.domain == "development"
        assert sdk_task.id.name == "recipes.aaa.simple.join_strings"
        assert sdk_task.id.version == "553018f39e519bdb2597b652639c30ce16b99c79"


def test_ref_task_more():
    @task(
        task_config=TaskReference(
            project="flytesnacks",
            domain="development",
            name="recipes.aaa.simple.join_strings",
            version="553018f39e519bdb2597b652639c30ce16b99c79",
        )
    )
    def ref_t1(a: typing.List[str]) -> str:
        ...

    @workflow
    def wf1(in1: typing.List[str]) -> str:
        return ref_t1(a=in1)

    with pytest.raises(Exception) as e:
        wf1(in1=["hello", "world"])
    assert "Remote reference tasks cannot be run" in f"{e}"

    with task_mock(ref_t1) as mock:
        mock.return_value = "hello"
        assert wf1(in1=["hello", "world"]) == "hello"


def test_fdsj():
    ref_lp = get_reference_entity(_identifier_model.ResourceType.LAUNCH_PLAN, "proj", "dom", "app.other.lp", "123",
                                  inputs=kwtypes(a=str, b=int), outputs={})


