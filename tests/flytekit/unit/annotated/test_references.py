import typing

import pytest

from flytekit.annotated import context_manager
from flytekit.annotated.base_task import kwtypes
from flytekit.annotated.context_manager import Image, ImageConfig
from flytekit.annotated.promise import VoidPromise
from flytekit.annotated.reference import get_reference_entity
from flytekit.annotated.reference_entity import ReferenceEntity, TaskReference, WorkflowReference
from flytekit.annotated.reference_task import reference_task
from flytekit.annotated.task import task
from flytekit.annotated.testing import patch, task_mock
from flytekit.annotated.workflow import workflow
from flytekit.models.core import identifier as _identifier_model


def test_ref():
    @reference_task(
        project="flytesnacks",
        domain="development",
        name="recipes.aaa.simple.join_strings",
        version="553018f39e519bdb2597b652639c30ce16b99c79",
    )
    def ref_t1(a: typing.List[str]) -> str:
        ...

    assert ref_t1.id.project == "flytesnacks"
    assert ref_t1.id.domain == "development"
    assert ref_t1.id.name == "recipes.aaa.simple.join_strings"
    assert ref_t1.id.version == "553018f39e519bdb2597b652639c30ce16b99c79"

    with context_manager.FlyteContext.current_context().new_registration_settings(
        registration_settings=context_manager.RegistrationSettings(
            project="test_proj",
            domain="test_domain",
            version="abc",
            image_config=ImageConfig(Image(name="name", fqn="image", tag="name")),
            env={},
        )
    ):
        ss = ref_t1.get_registerable_entity()
        assert ss.id == ref_t1.id
        assert ss.interface.inputs["a"] is not None
        assert ss.interface.outputs["o0"] is not None

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
    @reference_task(
        project="flytesnacks",
        domain="development",
        name="recipes.aaa.simple.join_strings",
        version="553018f39e519bdb2597b652639c30ce16b99c79",
    )
    def ref_t1(a: typing.List[str]) -> str:
        ...

    @workflow
    def wf1(in1: typing.List[str]) -> str:
        return ref_t1(a=in1)

    with pytest.raises(Exception) as e:
        wf1(in1=["hello", "world"])
    assert "You must mock this out" in f"{e}"

    with task_mock(ref_t1) as mock:
        mock.return_value = "hello"
        assert wf1(in1=["hello", "world"]) == "hello"


def test_reference_workflow():
    @task
    def t1(a: int) -> typing.NamedTuple("OutputsBC", t1_int_output=int, c=str):
        a = a + 2
        return a, "world-" + str(a)

    @workflow(reference=WorkflowReference(project="proj", domain="developement", name="wf_name", version="abc"))
    def ref_wf1(a: int) -> (str, str):
        ...

    @workflow
    def my_wf(a: int, b: str) -> (int, str, str):
        x, y = t1(a=a).with_overrides()
        u, v = ref_wf1(a=x)
        return x, u, v

    with pytest.raises(Exception):
        my_wf(a=3, b="foo")

    @patch(ref_wf1)
    def inner_test(ref_mock):
        ref_mock.return_value = ("hello", "alice")
        x, y, z = my_wf(a=3, b="foo")
        assert x == 5
        assert y == "hello"
        assert z == "alice"

    inner_test()

    # Ensure that the patching is only for the duration of that test
    with pytest.raises(Exception):
        my_wf(a=3, b="foo")


def test_ref_plain_no_outputs():
    r1 = ReferenceEntity(TaskReference("proj", "domain", "some.name", "abc"), inputs=kwtypes(a=str, b=int), outputs={},)

    # Reference entities should always raise an exception when not mocked out.
    with pytest.raises(Exception) as e:
        r1(a="fdsa", b=3)
    assert "You must mock this out" in f"{e}"

    @workflow
    def wf1(a: str, b: int):
        r1(a=a, b=b)

    @patch(r1)
    def inner_test(ref_mock):
        ref_mock.return_value = None
        x = wf1(a="fdsa", b=3)
        assert x is None

    inner_test()

    nt1 = typing.NamedTuple("DummyNamedTuple", t1_int_output=int, c=str)

    @task
    def t1(a: int) -> nt1:
        a = a + 2
        return a, "world-" + str(a)

    @workflow
    def wf2(a: int):
        t1_int, c = t1(a=a)
        r1(a=c, b=t1_int)

    @patch(r1)
    def inner_test2(ref_mock):
        ref_mock.return_value = None
        x = wf2(a=3)
        assert x is None
        ref_mock.assert_called_with(a="world-5", b=5)

    inner_test2()

    # Test nodes
    node_r1 = wf2._nodes[1]
    assert node_r1._upstream_nodes[0] is wf2._nodes[0]


def test_ref_plain_two_outputs():
    r1 = ReferenceEntity(
        TaskReference("proj", "domain", "some.name", "abc"),
        inputs=kwtypes(a=str, b=int),
        outputs=kwtypes(x=bool, y=int),
    )

    ctx = context_manager.FlyteContext.current_context()
    with ctx.new_compilation_context():
        xx, yy = r1(a="five", b=6)
        # Note - misnomer, these are not SdkNodes, they are annotated.Nodes
        assert xx.ref.sdk_node is yy.ref.sdk_node
        assert xx.var == "x"
        assert yy.var == "y"
        assert xx.ref.node_id == "n0"
        assert len(xx.ref.sdk_node.bindings) == 2

    @task
    def t2(q: bool, r: int) -> str:
        return f"q: {q} r: {r}"

    @workflow
    def wf1(a: str, b: int) -> str:
        x_out, y_out = r1(a=a, b=b)
        return t2(q=x_out, r=y_out)

    @patch(r1)
    def inner_test(ref_mock):
        ref_mock.return_value = (False, 30)
        x = wf1(a="hello", b=10)
        assert x == "q: False r: 30"

    inner_test()


@pytest.mark.parametrize(
    "resource_type",
    [
        _identifier_model.ResourceType.LAUNCH_PLAN,
        _identifier_model.ResourceType.TASK,
        _identifier_model.ResourceType.WORKFLOW,
    ],
)
def test_lps(resource_type):
    ref_entity = get_reference_entity(
        resource_type, "proj", "dom", "app.other.flyte_entity", "123", inputs=kwtypes(a=str, b=int), outputs={},
    )

    ctx = context_manager.FlyteContext.current_context()
    with pytest.raises(Exception) as e:
        ref_entity()
    assert "You must mock this out" in f"{e}"

    with ctx.new_compilation_context() as ctx:
        with pytest.raises(Exception) as e:
            ref_entity()
        assert "Input was not specified" in f"{e}"

        output = ref_entity(a="hello", b=3)
        assert isinstance(output, VoidPromise)

    @workflow
    def wf1(a: str, b: int):
        ref_entity(a=a, b=b)

    with context_manager.FlyteContext.current_context().new_registration_settings(
        registration_settings=context_manager.RegistrationSettings(
            project="test_proj",
            domain="test_domain",
            version="abc",
            image_config=ImageConfig(Image(name="name", fqn="image", tag="name")),
            env={},
        )
    ):
        sdk_wf = wf1.get_registerable_entity()
        assert len(sdk_wf.interface.inputs) == 2
        assert len(sdk_wf.interface.outputs) == 0
        assert len(sdk_wf.nodes) == 1
        if resource_type == _identifier_model.ResourceType.LAUNCH_PLAN:
            assert sdk_wf.nodes[0].workflow_node.launchplan_ref.project == "proj"
            assert sdk_wf.nodes[0].workflow_node.launchplan_ref.name == "app.other.flyte_entity"
        elif resource_type == _identifier_model.ResourceType.WORKFLOW:
            assert sdk_wf.nodes[0].workflow_node.sub_workflow_ref.project == "proj"
            assert sdk_wf.nodes[0].workflow_node.sub_workflow_ref.name == "app.other.flyte_entity"
        else:
            assert sdk_wf.nodes[0].task_node.reference_id.project == "proj"
            assert sdk_wf.nodes[0].task_node.reference_id.name == "app.other.flyte_entity"


def test_lp_with_output():
    ref_lp = get_reference_entity(
        _identifier_model.ResourceType.LAUNCH_PLAN,
        "proj",
        "dom",
        "app.other.flyte_entity",
        "123",
        inputs=kwtypes(a=str, b=int),
        outputs=kwtypes(x=bool, y=int),
    )

    @task
    def t1() -> (str, int):
        return "hello", 88

    @task
    def t2(q: bool, r: int) -> str:
        return f"q: {q} r: {r}"

    @workflow
    def wf1() -> str:
        t1_str, t1_int = t1()
        x_out, y_out = ref_lp(a=t1_str, b=t1_int)
        return t2(q=x_out, r=y_out)

    @patch(ref_lp)
    def inner_test(ref_mock):
        ref_mock.return_value = (False, 30)
        x = wf1()
        assert x == "q: False r: 30"
        ref_mock.assert_called_with(a="hello", b=88)

    inner_test()

    with context_manager.FlyteContext.current_context().new_registration_settings(
        registration_settings=context_manager.RegistrationSettings(
            project="test_proj",
            domain="test_domain",
            version="abc",
            image_config=ImageConfig(Image(name="name", fqn="image", tag="name")),
            env={},
        )
    ):
        sdk_wf = wf1.get_registerable_entity()
        assert sdk_wf.nodes[1].workflow_node.launchplan_ref.project == "proj"
        assert sdk_wf.nodes[1].workflow_node.launchplan_ref.name == "app.other.flyte_entity"
