import typing
from collections import OrderedDict

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from typing_extensions import Annotated  # type: ignore

import flytekit.configuration
from flytekit import FlyteContextManager, StructuredDataset, kwtypes
from flytekit.configuration import Image, ImageConfig
from flytekit.core import context_manager
from flytekit.core.condition import conditional
from flytekit.core.task import task
from flytekit.core.workflow import WorkflowFailurePolicy, WorkflowMetadata, WorkflowMetadataDefaults, workflow
from flytekit.exceptions.user import FlyteValidationException, FlyteValueException
from flytekit.tools.translator import get_serializable
from flytekit.types.schema import FlyteSchema

default_img = Image(name="default", fqn="test", tag="tag")
serialization_settings = flytekit.configuration.SerializationSettings(
    project="project",
    domain="domain",
    version="version",
    env=None,
    image_config=ImageConfig(default_image=default_img, images=[default_img]),
)


def test_metadata_values():
    with pytest.raises(FlyteValidationException):
        WorkflowMetadata(on_failure=0)

    wm = WorkflowMetadata(on_failure=WorkflowFailurePolicy.FAIL_IMMEDIATELY)
    assert wm.on_failure == WorkflowFailurePolicy.FAIL_IMMEDIATELY


def test_default_metadata_values():
    with pytest.raises(FlyteValidationException):
        WorkflowMetadataDefaults(3)

    wm = WorkflowMetadataDefaults(interruptible=False)
    assert wm.interruptible is False


def test_workflow_values():
    @task
    def t1(a: int) -> typing.NamedTuple("OutputsBC", [("t1_int_output", int), ("c", str)]):
        a = a + 2
        return a, "world-" + str(a)

    @workflow(interruptible=True, failure_policy=WorkflowFailurePolicy.FAIL_AFTER_EXECUTABLE_NODES_COMPLETE)
    def wf(a: int) -> typing.Tuple[str, str]:
        x, y = t1(a=a)
        u, v = t1(a=x)
        return y, v

    wf_spec = get_serializable(OrderedDict(), serialization_settings, wf)
    assert wf_spec.template.metadata_defaults.interruptible
    assert wf_spec.template.metadata.on_failure == 1


def test_default_values():
    @task
    def t() -> bool:
        return True

    @task
    def f() -> bool:
        return False

    @workflow
    def wf(a: bool = True) -> bool:
        return conditional("bool").if_(a.is_true()).then(t()).else_().then(f())  # type: ignore

    assert wf() is True
    assert wf(a=False) is False


def test_list_output_wf():
    @task
    def t1(a: int) -> int:
        a = a + 5
        return a

    @workflow
    def list_output_wf() -> typing.List[int]:
        v = []
        for i in range(2):
            v.append(t1(a=i))
        return v

    x = list_output_wf()
    assert x == [5, 6]


def test_sub_wf_single_named_tuple():
    nt = typing.NamedTuple("SingleNamedOutput", [("named1", int)])

    @task
    def t1(a: int) -> nt:
        a = a + 2
        return nt(a)

    @workflow
    def subwf(a: int) -> nt:
        return t1(a=a)

    @workflow
    def wf(b: int) -> nt:
        out = subwf(a=b)
        return t1(a=out.named1)

    x = wf(b=3)
    assert x == (7,)


def test_sub_wf_multi_named_tuple():
    nt = typing.NamedTuple("Multi", [("named1", int), ("named2", int)])

    @task
    def t1(a: int) -> nt:
        a = a + 2
        return nt(a, a)

    @workflow
    def subwf(a: int) -> nt:
        return t1(a=a)

    @workflow
    def wf(b: int) -> nt:
        out = subwf(a=b)
        return t1(a=out.named1)

    x = wf(b=3)
    assert x == (7, 7)


def test_unexpected_outputs():
    @task
    def t1(a: int) -> int:
        a = a + 5
        return a

    @workflow
    def no_outputs_wf():
        return t1(a=3)

    # Should raise an exception because the workflow returns something when it shouldn't
    with pytest.raises(FlyteValueException):
        no_outputs_wf()

    # Should raise an exception because it doesn't return something when it should
    with pytest.raises(AssertionError):

        @workflow
        def one_output_wf() -> int:  # type: ignore
            t1(a=3)

        one_output_wf()


def test_wf_no_output():
    @task
    def t1(a: int) -> int:
        a = a + 5
        return a

    @workflow
    def no_outputs_wf():
        t1(a=3)

    assert no_outputs_wf() is None


def test_wf_nested_comp():
    @task
    def t1(a: int) -> int:
        a = a + 5
        return a

    @workflow
    def outer() -> typing.Tuple[int, int]:
        # You should not do this. This is just here for testing.
        @workflow
        def wf2() -> int:
            return t1(a=5)

        return t1(a=3), wf2()

    assert (8, 10) == outer()
    entity_mapping = OrderedDict()

    model_wf = get_serializable(entity_mapping, serialization_settings, outer)

    assert len(model_wf.template.interface.outputs) == 2
    assert len(model_wf.template.nodes) == 2
    assert model_wf.template.nodes[1].workflow_node is not None

    sub_wf = model_wf.sub_workflows[0]
    assert len(sub_wf.nodes) == 1
    assert sub_wf.nodes[0].id == "n0"
    assert sub_wf.nodes[0].task_node.reference_id.name == "tests.flytekit.unit.core.test_workflows.t1"


@task
def add_5(a: int) -> int:
    a = a + 5
    return a


@workflow
def simple_wf() -> int:
    return add_5(a=1)


@workflow
def my_wf_example(a: int) -> typing.Tuple[int, int]:
    """example

    Workflows can have inputs and return outputs of simple or complex types.

    :param a: input a
    :return: outputs
    """

    x = add_5(a=a)

    # You can use outputs of a previous task as inputs to other nodes.
    z = add_5(a=x)

    # You can call other workflows from within this workflow
    d = simple_wf()

    # You can add conditions that can run on primitive types and execute different branches
    e = conditional("bool").if_(a == 5).then(add_5(a=d)).else_().then(add_5(a=z))

    # Outputs of the workflow have to be outputs returned by prior nodes.
    # No outputs and single or multiple outputs are supported
    return x, e


def test_all_node_types():
    assert my_wf_example(a=1) == (6, 16)
    entity_mapping = OrderedDict()

    model_wf = get_serializable(entity_mapping, serialization_settings, my_wf_example)

    assert len(model_wf.template.interface.outputs) == 2
    assert len(model_wf.template.nodes) == 4
    assert model_wf.template.nodes[2].workflow_node is not None

    sub_wf = model_wf.sub_workflows[0]
    assert len(sub_wf.nodes) == 1
    assert sub_wf.nodes[0].id == "n0"
    assert sub_wf.nodes[0].task_node.reference_id.name == "tests.flytekit.unit.core.test_workflows.add_5"


def test_wf_docstring():
    model_wf = get_serializable(OrderedDict(), serialization_settings, my_wf_example)

    assert len(model_wf.template.interface.outputs) == 2
    assert model_wf.template.interface.outputs["o0"].description == "outputs"
    assert model_wf.template.interface.outputs["o1"].description == "outputs"
    assert len(model_wf.template.interface.inputs) == 1
    assert model_wf.template.interface.inputs["a"].description == "input a"


superset_cols = kwtypes(Name=str, Age=int, Height=int)
subset_cols = kwtypes(Name=str)
superset_df = pd.DataFrame({"Name": ["Tom", "Joseph"], "Age": [20, 22], "Height": [160, 178]})
subset_df = pd.DataFrame({"Name": ["Tom", "Joseph"]})


@task
def t1() -> Annotated[pd.DataFrame, superset_cols]:
    return superset_df


@task
def t2(df: Annotated[pd.DataFrame, subset_cols]) -> Annotated[pd.DataFrame, subset_cols]:
    return df


@task
def t3(df: FlyteSchema[superset_cols]) -> FlyteSchema[superset_cols]:
    return df


@task
def t4() -> FlyteSchema[superset_cols]:
    return superset_df


@task
def t5(sd: Annotated[StructuredDataset, subset_cols]) -> Annotated[pd.DataFrame, subset_cols]:
    return sd.open(pd.DataFrame).all()


@workflow
def sd_wf() -> Annotated[pd.DataFrame, subset_cols]:
    # StructuredDataset -> StructuredDataset
    df = t1()
    return t2(df=df)


@workflow
def sd_to_schema_wf() -> pd.DataFrame:
    # StructuredDataset -> schema
    df = t1()
    return t3(df=df)


@workflow
def schema_to_sd_wf() -> typing.Tuple[pd.DataFrame, pd.DataFrame]:
    # schema -> StructuredDataset
    df = t4()
    return t2(df=df), t5(sd=df)  # type: ignore


def test_structured_dataset_wf():
    assert_frame_equal(sd_wf(), subset_df)
    assert_frame_equal(sd_to_schema_wf(), superset_df)
    assert_frame_equal(schema_to_sd_wf()[0], subset_df)
    assert_frame_equal(schema_to_sd_wf()[1], subset_df)


def test_compile_wf_at_compile_time():
    ctx = FlyteContextManager.current_context()
    with FlyteContextManager.with_context(
        ctx.with_execution_state(
            ctx.new_execution_state().with_params(mode=context_manager.ExecutionState.Mode.TASK_EXECUTION)
        )
    ):

        @workflow
        def wf():
            t4()

        assert ctx.compilation_state is None
