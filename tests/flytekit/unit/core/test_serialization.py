import os
import typing
from collections import OrderedDict

import mock
import pytest

import flytekit.configuration
from flytekit import ContainerTask, ImageSpec, kwtypes
from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.core.condition import conditional
from flytekit.core.python_auto_container import get_registerable_container_image
from flytekit.core.task import task
from flytekit.core.workflow import workflow
from flytekit.exceptions.user import FlyteAssertion
from flytekit.image_spec.image_spec import ImageBuildEngine, _calculate_deduped_hash_from_image_spec
from flytekit.models.admin.workflow import WorkflowSpec
from flytekit.models.literals import (
    BindingData,
    BindingDataCollection,
    BindingDataMap,
    Literal,
    Primitive,
    Scalar,
    Union,
    Void,
)
from flytekit.models.types import LiteralType, SimpleType, TypeStructure, UnionType
from flytekit.tools.translator import get_serializable
from flytekit.types.error.error import FlyteError

default_img = Image(name="default", fqn="test", tag="tag")
serialization_settings = flytekit.configuration.SerializationSettings(
    project="project",
    domain="domain",
    version="version",
    env=None,
    image_config=ImageConfig(default_image=default_img, images=[default_img]),
)


def test_serialization():
    square = ContainerTask(
        name="square",
        input_data_dir="/var/inputs",
        output_data_dir="/var/outputs",
        inputs=kwtypes(val=int),
        outputs=kwtypes(out=int),
        image="alpine",
        environment={"a": "b"},
        command=["sh", "-c", "echo $(( {{.Inputs.val}} * {{.Inputs.val}} )) | tee /var/outputs/out"],
    )

    sum = ContainerTask(
        name="sum",
        input_data_dir="/var/flyte/inputs",
        output_data_dir="/var/flyte/outputs",
        inputs=kwtypes(x=int, y=int),
        outputs=kwtypes(out=int),
        image="alpine",
        command=["sh", "-c", "echo $(( {{.Inputs.x}} + {{.Inputs.y}} )) | tee /var/flyte/outputs/out"],
    )

    @task()
    def clean_up(val1: int, val2: int, err: typing.Optional[FlyteError] = None):
        print("Deleting the cluster")

    @workflow(on_failure=clean_up)
    def subwf(val1: int, val2: int) -> int:
        return sum(x=square(val=val1), y=square(val=val2))

    @workflow(on_failure=clean_up)
    def raw_container_wf(val1: int, val2: int) -> int:
        subwf(val1=val1, val2=val2)
        return sum(x=square(val=val1), y=square(val=val2))

    default_img = Image(name="default", fqn="test", tag="tag")
    serialization_settings = flytekit.configuration.SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env=None,
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )
    wf_spec = typing.cast(WorkflowSpec, get_serializable(OrderedDict(), serialization_settings, raw_container_wf))
    assert wf_spec is not None
    assert wf_spec.template is not None
    assert len(wf_spec.template.nodes) == 4
    assert wf_spec.template.failure_node is not None
    assert wf_spec.template.failure_node.task_node is not None
    assert wf_spec.template.failure_node.id == "fn0"
    assert wf_spec.sub_workflows[0].failure_node is not None
    sqn_spec = get_serializable(OrderedDict(), serialization_settings, square)
    assert sqn_spec.template.container.image == "alpine"
    sum_spec = get_serializable(OrderedDict(), serialization_settings, sum)
    assert sum_spec.template.container.image == "alpine"


def test_serialization_branch_complex():
    @task
    def t1(a: int) -> typing.NamedTuple("OutputsBC", t1_int_output=int, c=str):
        return a + 2, "world"

    @task
    def t2(a: str) -> str:
        return a

    @workflow
    def my_wf(a: int, b: str) -> (int, str):
        x, y = t1(a=a)
        d = (
            conditional("test1")
            .if_(x == 4)
            .then(t2(a=b))
            .elif_(x >= 5)
            .then(t2(a=y))
            .else_()
            .fail("Unable to choose branch")
        )
        f = conditional("test2").if_(d == "hello ").then(t2(a="It is hello")).else_().then(t2(a="Not Hello!"))
        return x, f

    wf_spec = get_serializable(OrderedDict(), serialization_settings, my_wf)
    assert wf_spec is not None
    assert len(wf_spec.template.nodes) == 3
    assert wf_spec.template.nodes[1].branch_node is not None
    assert wf_spec.template.nodes[2].branch_node is not None


def test_serialization_branch_sub_wf():
    @task
    def t1(a: int) -> int:
        return a + 2

    @workflow
    def my_sub_wf(a: int) -> int:
        return t1(a=a)

    @workflow
    def my_wf(a: int) -> int:
        d = conditional("test1").if_(a > 3).then(t1(a=a)).else_().then(my_sub_wf(a=a))
        return d

    wf_spec = get_serializable(OrderedDict(), serialization_settings, my_wf)
    assert wf_spec is not None
    assert len(wf_spec.template.nodes[0].inputs) == 1
    assert wf_spec.template.nodes[0].inputs[0].var == ".a"
    assert wf_spec.template.nodes[0] is not None


def test_serialization_branch_compound_conditions():
    @task
    def t1(a: int) -> int:
        return a + 2

    @workflow
    def my_wf(a: int) -> int:
        d = (
            conditional("test1")
            .if_((a == 4) | (a == 3))
            .then(t1(a=a))
            .elif_(a < 6)
            .then(t1(a=a))
            .else_()
            .fail("Unable to choose branch")
        )
        return d

    default_img = Image(name="default", fqn="test", tag="tag")
    serialization_settings = flytekit.configuration.SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env=None,
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )
    wf_spec = get_serializable(OrderedDict(), serialization_settings, my_wf)
    assert wf_spec is not None
    assert len(wf_spec.template.nodes[0].inputs) == 1
    assert wf_spec.template.nodes[0].inputs[0].var == ".a"


def test_serialization_branch_complex_2():
    @task
    def t1(a: int) -> typing.NamedTuple("OutputsBC", t1_int_output=int, c=str):
        return a + 2, "world"

    @task
    def t2(a: str) -> str:
        return a

    @workflow
    def my_wf(a: int, b: str) -> (int, str):
        x, y = t1(a=a)
        d = (
            conditional("test1")
            .if_(x == 4)
            .then(t2(a=b))
            .elif_(x >= 5)
            .then(t2(a=y))
            .else_()
            .fail("Unable to choose branch")
        )
        f = conditional("test2").if_(d == "hello ").then(t2(a="It is hello")).else_().then(t2(a="Not Hello!"))
        return x, f

    default_img = Image(name="default", fqn="test", tag="tag")
    serialization_settings = flytekit.configuration.SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env=None,
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )
    wf_spec = get_serializable(OrderedDict(), serialization_settings, my_wf)
    assert wf_spec is not None
    assert wf_spec.template.nodes[1].inputs[0].var == "n0.t1_int_output"


def test_serialization_branch():
    @task
    def mimic(a: int) -> typing.NamedTuple("OutputsBC", c=int):
        return (a,)

    @task
    def t1(c: int) -> typing.NamedTuple("OutputsBC", c=str):
        return ("world",)

    @task
    def t2() -> typing.NamedTuple("OutputsBC", c=str):
        return ("hello",)

    @workflow
    def my_wf(a: int) -> str:
        c = mimic(a=a)
        return conditional("test1").if_(c.c == 4).then(t1(c=c.c).c).else_().then(t2().c)

    assert my_wf(a=4) == "world"
    assert my_wf(a=2) == "hello"

    default_img = Image(name="default", fqn="test", tag="tag")
    serialization_settings = flytekit.configuration.SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env=None,
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )
    wf_spec = get_serializable(OrderedDict(), serialization_settings, my_wf)
    assert wf_spec is not None
    assert len(wf_spec.template.nodes) == 2
    assert wf_spec.template.nodes[1].branch_node is not None


def test_bad_configuration():
    container_image = "{{.image.xyz.fqn}}:{{.image.default.version}}"
    image_config = ImageConfig.auto(
        config_file=os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs/images.config")
    )
    # No default image in the images.config file so nothing to pull version from
    with pytest.raises(AssertionError):
        get_registerable_container_image(container_image, image_config)


def test_serialization_images(mock_image_spec_builder):
    ImageBuildEngine.register("test", mock_image_spec_builder)

    @task(container_image="{{.image.xyz.fqn}}:{{.image.xyz.version}}")
    def t1(a: int) -> int:
        return a

    @task(container_image="{{.image.abc.fqn}}:{{.image.xyz.version}}")
    def t2():
        pass

    @task(container_image="docker.io/org/myimage:latest")
    def t4():
        pass

    @task(container_image="docker.io/org/myimage:{{.image.xyz.version}}")
    def t5(a: int) -> int:
        return a

    @task(container_image="{{.image.xyz_123.fqn}}:{{.image.xyz_123.version}}")
    def t6(a: int) -> int:
        return a

    image_spec = ImageSpec(
        packages=["mypy"],
        apt_packages=["git"],
        registry="ghcr.io/flyteorg",
        builder="test",
    )

    @task(container_image=image_spec)
    def t7(a: int) -> int:
        return a

    with mock.patch.dict(os.environ, {"FLYTE_INTERNAL_IMAGE": "docker.io/default:version"}):
        imgs = ImageConfig.auto(
            config_file=os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs/images.config")
        )
        imgs.images.append(
            Image(name=_calculate_deduped_hash_from_image_spec(image_spec), fqn="docker.io/t7", tag="latest")
        )
        rs = flytekit.configuration.SerializationSettings(
            project="project",
            domain="domain",
            version="version",
            env=None,
            image_config=imgs,
        )
        t1_spec = get_serializable(OrderedDict(), rs, t1)
        assert t1_spec.template.container.image == "docker.io/xyz:latest"
        t1_spec.to_flyte_idl()

        t2_spec = get_serializable(OrderedDict(), rs, t2)
        assert t2_spec.template.container.image == "docker.io/abc:latest"

        t4_spec = get_serializable(OrderedDict(), rs, t4)
        assert t4_spec.template.container.image == "docker.io/org/myimage:latest"

        t5_spec = get_serializable(OrderedDict(), rs, t5)
        assert t5_spec.template.container.image == "docker.io/org/myimage:latest"

        t6_spec = get_serializable(OrderedDict(), rs, t6)
        assert t6_spec.template.container.image == "docker.io/xyz_123:v1"

        t7_spec = get_serializable(OrderedDict(), rs, t7)
        assert t7_spec.template.container.image == "docker.io/t7:latest"


def test_serialization_command1():
    @task
    def t1(a: str) -> str:
        return a

    task_spec = get_serializable(OrderedDict(), serialization_settings, t1)
    assert task_spec.template.container.args[-7:] == [
        "--resolver",
        "flytekit.core.python_auto_container.default_task_resolver",
        "--",
        "task-module",
        "tests.flytekit.unit.core.test_serialization",
        # when unit testing, t1.task_function.__module__ just gives this file
        "task-name",
        "t1",
    ]


def test_serialization_types():
    @task(cache=True, cache_version="1.0.0")
    def squared(value: int) -> typing.List[typing.Dict[str, int]]:
        return [
            {"squared_value": value**2},
        ]

    @workflow
    def compute_square_wf(input_integer: int) -> typing.List[typing.Dict[str, int]]:
        compute_square_result = squared(value=input_integer)
        return compute_square_result

    wf_spec = get_serializable(OrderedDict(), serialization_settings, compute_square_wf)
    assert wf_spec.template.interface.outputs["o0"].type.collection_type.map_value_type.simple == SimpleType.INTEGER
    task_spec = get_serializable(OrderedDict(), serialization_settings, squared)
    assert task_spec.template.interface.outputs["o0"].type.collection_type.map_value_type.simple == SimpleType.INTEGER


def test_serialization_named_return():
    @task
    def t1() -> str:
        return "Hello"

    @workflow
    def wf() -> typing.NamedTuple("OP", a=str, b=str):
        return t1(), t1()

    wf_spec = get_serializable(OrderedDict(), serialization_settings, wf)
    assert len(wf_spec.template.interface.outputs) == 2
    assert list(wf_spec.template.interface.outputs.keys()) == ["a", "b"]


def test_serialization_set_command():
    @task
    def t1() -> str:
        return "Hello"

    def new_command_fn(settings: SerializationSettings) -> typing.List[str]:
        return ["echo", "hello", "world"]

    t1.set_command_fn(new_command_fn)
    custom_command = t1.get_command(serialization_settings)
    assert ["echo", "hello", "world"] == custom_command
    t1.reset_command_fn()
    custom_command = t1.get_command(serialization_settings)
    assert custom_command[0] == "pyflyte-execute"


def test_serialization_nested_subwf():
    @task
    def t1(a: int) -> int:
        return a + 2

    @workflow
    def leaf_subwf(a: int = 42) -> typing.Tuple[int, int]:
        x = t1(a=a)
        u = t1(a=x)
        return x, u

    @workflow
    def middle_subwf() -> typing.Tuple[int, int]:
        s1, s2 = leaf_subwf(a=50)
        return s2, s2

    @workflow
    def parent_wf() -> typing.Tuple[int, int, int, int]:
        m1, m2 = middle_subwf()
        l1, l2 = leaf_subwf().with_overrides(node_name="foo-node")
        return m1, m2, l1, l2

    wf_spec = get_serializable(OrderedDict(), serialization_settings, parent_wf)
    assert wf_spec is not None
    assert len(wf_spec.sub_workflows) == 2
    subwf = {v.id.name: v for v in wf_spec.sub_workflows}
    assert subwf.keys() == {
        "tests.flytekit.unit.core.test_serialization.leaf_subwf",
        "tests.flytekit.unit.core.test_serialization.middle_subwf",
    }
    midwf = subwf["tests.flytekit.unit.core.test_serialization.middle_subwf"]
    assert len(midwf.nodes) == 1
    assert midwf.nodes[0].workflow_node is not None
    assert (
        midwf.nodes[0].workflow_node.sub_workflow_ref.name == "tests.flytekit.unit.core.test_serialization.leaf_subwf"
    )
    assert wf_spec.template.nodes[1].id == "foo-node"
    assert wf_spec.template.outputs[2].binding.promise.node_id == "foo-node"


def test_serialization_named_outputs_single():
    @task
    def t1() -> typing.NamedTuple("OP", a=str):
        return "Hello"

    @workflow
    def wf() -> typing.NamedTuple("OP", a=str):
        return t1().a

    wf_spec = get_serializable(OrderedDict(), serialization_settings, wf)
    assert len(wf_spec.template.interface.outputs) == 1
    assert list(wf_spec.template.interface.outputs.keys()) == ["a"]
    a = wf()
    assert a.a == "Hello"


def test_named_outputs_nested():
    nm = typing.NamedTuple("OP", [("greet", str)])

    @task
    def say_hello() -> nm:
        return nm("hello world")

    wf_outputs = typing.NamedTuple("OP2", [("greet1", str), ("greet2", str)])

    @workflow
    def my_wf() -> wf_outputs:
        # Note only Namedtuple can be created like this
        return wf_outputs(say_hello().greet, say_hello().greet)

    x, y = my_wf()
    assert x == "hello world"
    assert y == "hello world"


def test_named_outputs_nested_fail():
    nm = typing.NamedTuple("OP", [("greet", str)])

    @task
    def say_hello() -> nm:
        return nm("hello world")

    wf_outputs = typing.NamedTuple("OP2", [("greet1", str), ("greet2", str)])

    with pytest.raises(AssertionError):
        # this should fail because say_hello returns a tuple, but we do not de-reference it
        @workflow
        def my_wf() -> wf_outputs:
            # Note only Namedtuple can be created like this
            return wf_outputs(say_hello(), say_hello())

        my_wf()


def test_serialized_docstrings():
    @task
    def z(a: int, b: str) -> typing.Tuple[int, str]:
        """
        function z

        :param a: foo
        :param b: bar
        :return: ramen
        """
        ...

    task_spec = get_serializable(OrderedDict(), serialization_settings, z)
    assert task_spec.template.interface.inputs["a"].description == "foo"
    assert task_spec.template.interface.inputs["b"].description == "bar"
    assert task_spec.template.interface.outputs["o0"].description == "ramen"


def test_default_args_task_int_type():
    default_val = 0
    input_val = 100

    @task
    def t1(a: int = default_val) -> int:
        return a

    @workflow
    def wf_no_input() -> int:
        return t1()

    @workflow
    def wf_with_input() -> int:
        return t1(a=input_val)

    @workflow
    def wf_with_sub_wf() -> tuple[int, int]:
        return (wf_no_input(), wf_with_input())

    wf_no_input_spec = get_serializable(OrderedDict(), serialization_settings, wf_no_input)
    wf_with_input_spec = get_serializable(OrderedDict(), serialization_settings, wf_with_input)

    assert wf_no_input_spec.template.nodes[0].inputs[0].binding.value == Scalar(
        primitive=Primitive(integer=default_val)
    )
    assert wf_with_input_spec.template.nodes[0].inputs[0].binding.value == Scalar(
        primitive=Primitive(integer=input_val)
    )

    output_type = LiteralType(simple=SimpleType.INTEGER)
    assert wf_no_input_spec.template.interface.outputs["o0"].type == output_type
    assert wf_with_input_spec.template.interface.outputs["o0"].type == output_type

    assert wf_no_input() == default_val
    assert wf_with_input() == input_val
    assert wf_with_sub_wf() == (default_val, input_val)


def test_default_args_task_str_type():
    default_val = ""
    input_val = "foo"

    @task
    def t1(a: str = default_val) -> str:
        return a

    @workflow
    def wf_no_input() -> str:
        return t1()

    @workflow
    def wf_with_input() -> str:
        return t1(a=input_val)

    @workflow
    def wf_with_sub_wf() -> tuple[str, str]:
        return (wf_no_input(), wf_with_input())

    wf_no_input_spec = get_serializable(OrderedDict(), serialization_settings, wf_no_input)
    wf_with_input_spec = get_serializable(OrderedDict(), serialization_settings, wf_with_input)

    assert wf_no_input_spec.template.nodes[0].inputs[0].binding.value == Scalar(
        primitive=Primitive(string_value=default_val)
    )
    assert wf_with_input_spec.template.nodes[0].inputs[0].binding.value == Scalar(
        primitive=Primitive(string_value=input_val)
    )

    output_type = LiteralType(simple=SimpleType.STRING)
    assert wf_no_input_spec.template.interface.outputs["o0"].type == output_type
    assert wf_with_input_spec.template.interface.outputs["o0"].type == output_type

    assert wf_no_input() == default_val
    assert wf_with_input() == input_val
    assert wf_with_sub_wf() == (default_val, input_val)


def test_default_args_task_optional_int_type_default_none():
    default_val = None
    input_val = 100

    @task
    def t1(a: typing.Optional[int] = default_val) -> typing.Optional[int]:
        return a

    @workflow
    def wf_no_input() -> typing.Optional[int]:
        return t1()

    @workflow
    def wf_with_input() -> typing.Optional[int]:
        return t1(a=input_val)

    @workflow
    def wf_with_sub_wf() -> tuple[typing.Optional[int], typing.Optional[int]]:
        return (wf_no_input(), wf_with_input())

    wf_no_input_spec = get_serializable(OrderedDict(), serialization_settings, wf_no_input)
    wf_with_input_spec = get_serializable(OrderedDict(), serialization_settings, wf_with_input)

    assert wf_no_input_spec.template.nodes[0].inputs[0].binding.value == Scalar(
        union=Union(
            value=Literal(
                scalar=Scalar(
                    none_type=Void(),
                ),
            ),
            stored_type=LiteralType(
                simple=SimpleType.NONE,
                structure=TypeStructure(tag="none"),
            ),
        ),
    )
    assert wf_with_input_spec.template.nodes[0].inputs[0].binding.value == Scalar(
        union=Union(
            value=Literal(
                scalar=Scalar(primitive=Primitive(integer=input_val)),
            ),
            stored_type=LiteralType(
                simple=SimpleType.INTEGER,
                structure=TypeStructure(tag="int"),
            ),
        ),
    )

    output_type = LiteralType(
        union_type=UnionType(
            [
                LiteralType(
                    simple=SimpleType.INTEGER,
                    structure=TypeStructure(tag="int"),
                ),
                LiteralType(
                    simple=SimpleType.NONE,
                    structure=TypeStructure(tag="none"),
                ),
            ]
        )
    )
    assert wf_no_input_spec.template.interface.outputs["o0"].type == output_type
    assert wf_with_input_spec.template.interface.outputs["o0"].type == output_type

    assert wf_no_input() == default_val
    assert wf_with_input() == input_val
    assert wf_with_sub_wf() == (default_val, input_val)


def test_default_args_task_optional_int_type_default_int():
    default_val = 10
    input_val = 100

    @task
    def t1(a: typing.Optional[int] = default_val) -> typing.Optional[int]:
        return a

    @workflow
    def wf_no_input() -> typing.Optional[int]:
        return t1()

    @workflow
    def wf_with_input() -> typing.Optional[int]:
        return t1(a=input_val)

    @workflow
    def wf_with_sub_wf() -> tuple[typing.Optional[int], typing.Optional[int]]:
        return (wf_no_input(), wf_with_input())

    wf_no_input_spec = get_serializable(OrderedDict(), serialization_settings, wf_no_input)
    wf_with_input_spec = get_serializable(OrderedDict(), serialization_settings, wf_with_input)

    assert wf_no_input_spec.template.nodes[0].inputs[0].binding.value == Scalar(
        union=Union(
            value=Literal(
                scalar=Scalar(
                    primitive=Primitive(integer=default_val),
                ),
            ),
            stored_type=LiteralType(
                simple=SimpleType.INTEGER,
                structure=TypeStructure(tag="int"),
            ),
        ),
    )
    assert wf_with_input_spec.template.nodes[0].inputs[0].binding.value == Scalar(
        union=Union(
            value=Literal(
                scalar=Scalar(primitive=Primitive(integer=input_val)),
            ),
            stored_type=LiteralType(
                simple=SimpleType.INTEGER,
                structure=TypeStructure(tag="int"),
            ),
        ),
    )

    output_type = LiteralType(
        union_type=UnionType(
            [
                LiteralType(
                    simple=SimpleType.INTEGER,
                    structure=TypeStructure(tag="int"),
                ),
                LiteralType(
                    simple=SimpleType.NONE,
                    structure=TypeStructure(tag="none"),
                ),
            ]
        )
    )
    assert wf_no_input_spec.template.interface.outputs["o0"].type == output_type
    assert wf_with_input_spec.template.interface.outputs["o0"].type == output_type

    assert wf_no_input() == default_val
    assert wf_with_input() == input_val
    assert wf_with_sub_wf() == (default_val, input_val)


def test_default_args_task_no_type_hint():
    @task
    def t1(a=0) -> int:
        return a

    @workflow
    def wf_no_input() -> int:
        return t1()

    @workflow
    def wf_with_input() -> int:
        return t1(a=100)

    with pytest.raises(TypeError, match="Arguments do not have type annotation"):
        get_serializable(OrderedDict(), serialization_settings, wf_no_input)
    with pytest.raises(TypeError, match="Arguments do not have type annotation"):
        get_serializable(OrderedDict(), serialization_settings, wf_with_input)


def test_default_args_task_mismatch_type():
    @task
    def t1(a: int = "foo") -> int:  # type: ignore
        return a

    @workflow
    def wf_no_input() -> int:
        return t1()

    @workflow
    def wf_with_input() -> int:
        return t1(a="bar")

    with pytest.raises(AssertionError, match="Failed to Bind variable"):
        get_serializable(OrderedDict(), serialization_settings, wf_no_input)
    with pytest.raises(AssertionError, match="Failed to Bind variable"):
        get_serializable(OrderedDict(), serialization_settings, wf_with_input)


def test_default_args_task_list_type():
    input_val = [1, 2, 3]

    @task
    def t1(a: list[int] = []) -> list[int]:
        return a

    @workflow
    def wf_no_input() -> list[int]:
        return t1()

    @workflow
    def wf_with_input() -> list[int]:
        return t1(a=input_val)

    with pytest.raises(FlyteAssertion, match="Cannot use non-hashable object as default argument"):
        get_serializable(OrderedDict(), serialization_settings, wf_no_input)

    wf_with_input_spec = get_serializable(OrderedDict(), serialization_settings, wf_with_input)

    assert wf_with_input_spec.template.nodes[0].inputs[0].binding.value == BindingDataCollection(
        bindings=[
            BindingData(scalar=Scalar(primitive=Primitive(integer=1))),
            BindingData(scalar=Scalar(primitive=Primitive(integer=2))),
            BindingData(scalar=Scalar(primitive=Primitive(integer=3))),
        ]
    )

    assert wf_with_input_spec.template.interface.outputs["o0"].type == LiteralType(
        collection_type=LiteralType(simple=SimpleType.INTEGER)
    )

    assert wf_with_input() == input_val


def test_default_args_task_dict_type():
    input_val = {"a": 1, "b": 2}

    @task
    def t1(a: dict[str, int] = {}) -> dict[str, int]:
        return a

    @workflow
    def wf_no_input() -> dict[str, int]:
        return t1()

    @workflow
    def wf_with_input() -> dict[str, int]:
        return t1(a=input_val)

    with pytest.raises(FlyteAssertion, match="Cannot use non-hashable object as default argument"):
        get_serializable(OrderedDict(), serialization_settings, wf_no_input)

    wf_with_input_spec = get_serializable(OrderedDict(), serialization_settings, wf_with_input)

    assert wf_with_input_spec.template.nodes[0].inputs[0].binding.value == BindingDataMap(
        bindings={
            "a": BindingData(scalar=Scalar(primitive=Primitive(integer=1))),
            "b": BindingData(scalar=Scalar(primitive=Primitive(integer=2))),
        }
    )

    assert wf_with_input_spec.template.interface.outputs["o0"].type == LiteralType(
        map_value_type=LiteralType(simple=SimpleType.INTEGER)
    )

    assert wf_with_input() == input_val


def test_default_args_task_optional_list_type_default_none():
    default_val = None
    input_val = [1, 2, 3]

    @task
    def t1(a: typing.Optional[typing.List[int]] = default_val) -> typing.Optional[typing.List[int]]:
        return a

    @workflow
    def wf_no_input() -> typing.Optional[typing.List[int]]:
        return t1()

    @workflow
    def wf_with_input() -> typing.Optional[typing.List[int]]:
        return t1(a=input_val)

    @workflow
    def wf_with_sub_wf() -> tuple[typing.Optional[typing.List[int]], typing.Optional[typing.List[int]]]:
        return (wf_no_input(), wf_with_input())

    wf_no_input_spec = get_serializable(OrderedDict(), serialization_settings, wf_no_input)
    wf_with_input_spec = get_serializable(OrderedDict(), serialization_settings, wf_with_input)

    assert wf_no_input_spec.template.nodes[0].inputs[0].binding.value == Scalar(
        union=Union(
            value=Literal(
                scalar=Scalar(
                    none_type=Void(),
                ),
            ),
            stored_type=LiteralType(
                simple=SimpleType.NONE,
                structure=TypeStructure(tag="none"),
            ),
        ),
    )
    assert wf_with_input_spec.template.nodes[0].inputs[0].binding.value == BindingDataCollection(
        bindings=[
            BindingData(scalar=Scalar(primitive=Primitive(integer=1))),
            BindingData(scalar=Scalar(primitive=Primitive(integer=2))),
            BindingData(scalar=Scalar(primitive=Primitive(integer=3))),
        ]
    )

    output_type = LiteralType(
        union_type=UnionType(
            [
                LiteralType(
                    collection_type=LiteralType(simple=SimpleType.INTEGER),
                    structure=TypeStructure(tag="Typed List"),
                ),
                LiteralType(
                    simple=SimpleType.NONE,
                    structure=TypeStructure(tag="none"),
                ),
            ]
        )
    )
    assert wf_no_input_spec.template.interface.outputs["o0"].type == output_type
    assert wf_with_input_spec.template.interface.outputs["o0"].type == output_type

    assert wf_no_input() == default_val
    assert wf_with_input() == input_val
    assert wf_with_sub_wf() == (default_val, input_val)


def test_default_args_task_optional_list_type_default_list():
    input_val = [1, 2, 3]

    @task
    def t1(a: typing.Optional[typing.List[int]] = []) -> typing.Optional[typing.List[int]]:
        return a

    @workflow
    def wf_no_input() -> typing.Optional[typing.List[int]]:
        return t1()

    @workflow
    def wf_with_input() -> typing.Optional[typing.List[int]]:
        return t1(a=input_val)

    with pytest.raises(FlyteAssertion, match="Cannot use non-hashable object as default argument"):
        get_serializable(OrderedDict(), serialization_settings, wf_no_input)

    wf_with_input_spec = get_serializable(OrderedDict(), serialization_settings, wf_with_input)

    assert wf_with_input_spec.template.nodes[0].inputs[0].binding.value == BindingDataCollection(
        bindings=[
            BindingData(scalar=Scalar(primitive=Primitive(integer=1))),
            BindingData(scalar=Scalar(primitive=Primitive(integer=2))),
            BindingData(scalar=Scalar(primitive=Primitive(integer=3))),
        ]
    )

    assert wf_with_input_spec.template.interface.outputs["o0"].type == LiteralType(
        union_type=UnionType(
            [
                LiteralType(
                    collection_type=LiteralType(simple=SimpleType.INTEGER),
                    structure=TypeStructure(tag="Typed List"),
                ),
                LiteralType(
                    simple=SimpleType.NONE,
                    structure=TypeStructure(tag="none"),
                ),
            ]
        )
    )

    assert wf_with_input() == input_val

def test_positional_args_task():
    arg1 = 5
    arg2 = 6
    ret = 17

    @task
    def t1(x: int, y: int) -> int:
        return x + y * 2

    @workflow
    def wf_pure_positional_args() -> int:
        return t1(arg1, arg2)

    @workflow
    def wf_mixed_positional_and_keyword_args() -> int:
        return t1(arg1, y=arg2)

    wf_pure_positional_args_spec = get_serializable(OrderedDict(), serialization_settings, wf_pure_positional_args)
    wf_mixed_positional_and_keyword_args_spec = get_serializable(OrderedDict(), serialization_settings, wf_mixed_positional_and_keyword_args)

    arg1_binding = Scalar(primitive=Primitive(integer=arg1))
    arg2_binding = Scalar(primitive=Primitive(integer=arg2))
    output_type = LiteralType(simple=SimpleType.INTEGER)

    assert wf_pure_positional_args_spec.template.nodes[0].inputs[0].binding.value == arg1_binding
    assert wf_pure_positional_args_spec.template.nodes[0].inputs[1].binding.value == arg2_binding
    assert wf_pure_positional_args_spec.template.interface.outputs["o0"].type == output_type


    assert wf_mixed_positional_and_keyword_args_spec.template.nodes[0].inputs[0].binding.value == arg1_binding
    assert wf_mixed_positional_and_keyword_args_spec.template.nodes[0].inputs[1].binding.value == arg2_binding
    assert wf_mixed_positional_and_keyword_args_spec.template.interface.outputs["o0"].type == output_type

    assert wf_pure_positional_args() == ret
    assert wf_mixed_positional_and_keyword_args() == ret

def test_positional_args_workflow():
    arg1 = 5
    arg2 = 6
    ret = 17

    @task
    def t1(x: int, y: int) -> int:
        return x + y * 2

    @workflow
    def sub_wf(x: int, y: int) -> int:
        return t1(x=x, y=y)

    @workflow
    def wf_pure_positional_args() -> int:
        return sub_wf(arg1, arg2)

    @workflow
    def wf_mixed_positional_and_keyword_args() -> int:
        return sub_wf(arg1, y=arg2)

    wf_pure_positional_args_spec = get_serializable(OrderedDict(), serialization_settings, wf_pure_positional_args)
    wf_mixed_positional_and_keyword_args_spec = get_serializable(OrderedDict(), serialization_settings, wf_mixed_positional_and_keyword_args)

    arg1_binding = Scalar(primitive=Primitive(integer=arg1))
    arg2_binding = Scalar(primitive=Primitive(integer=arg2))
    output_type = LiteralType(simple=SimpleType.INTEGER)

    assert wf_pure_positional_args_spec.template.nodes[0].inputs[0].binding.value == arg1_binding
    assert wf_pure_positional_args_spec.template.nodes[0].inputs[1].binding.value == arg2_binding
    assert wf_pure_positional_args_spec.template.interface.outputs["o0"].type == output_type

    assert wf_mixed_positional_and_keyword_args_spec.template.nodes[0].inputs[0].binding.value == arg1_binding
    assert wf_mixed_positional_and_keyword_args_spec.template.nodes[0].inputs[1].binding.value == arg2_binding
    assert wf_mixed_positional_and_keyword_args_spec.template.interface.outputs["o0"].type == output_type

    assert wf_pure_positional_args() == ret
    assert wf_mixed_positional_and_keyword_args() == ret

def test_positional_args_chained_tasks():
    @task
    def t1(x: int, y: int) -> int:
        return x + y * 2

    @workflow
    def wf() -> int:
        x = t1(2, y = 3)
        y = t1(3, 4)
        return t1(x, y = y)

    assert wf() == 30

def test_positional_args_task_inputs_from_workflow_args():
    @task
    def t1(x: int, y: int, z: int) -> int:
        return x + y * 2 + z * 3

    @workflow
    def wf(x: int, y: int) -> int:
        return t1(x, y=y, z=3)

    assert wf(1, 2) == 14

def test_unexpected_kwargs_task_raises_error():
    @task
    def t1(a: int) -> int:
        return a

    with pytest.raises(AssertionError, match="Received unexpected keyword argument"):
        t1(b=6)

def test_too_many_positional_args_task_raises_error():
    @task
    def t1(a: int) -> int:
        return a

    with pytest.raises(AssertionError, match="Received more arguments than expected"):
        t1(1, 2)

def test_both_positional_and_keyword_args_task_raises_error():
    @task
    def t1(a: int) -> int:
        return a

    with pytest.raises(AssertionError, match="Got multiple values for argument"):
        t1(1, a=2)
