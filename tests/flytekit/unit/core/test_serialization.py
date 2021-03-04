import os
import typing

from flytekit import ContainerTask, kwtypes
from flytekit.common.translator import get_serializable
from flytekit.configuration import set_flyte_config_file
from flytekit.core import context_manager
from flytekit.core.condition import conditional
from flytekit.core.context_manager import Image, ImageConfig, get_image_config
from flytekit.core.task import task
from flytekit.core.workflow import workflow
from flytekit.models.types import SimpleType


def test_serialization():
    square = ContainerTask(
        name="square",
        input_data_dir="/var/inputs",
        output_data_dir="/var/outputs",
        inputs=kwtypes(val=int),
        outputs=kwtypes(out=int),
        image="alpine",
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

    @workflow
    def raw_container_wf(val1: int, val2: int) -> int:
        return sum(x=square(val=val1), y=square(val=val2))

    default_img = Image(name="default", fqn="test", tag="tag")
    serialization_settings = context_manager.SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env=None,
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )
    wf = get_serializable(serialization_settings, raw_container_wf)
    assert wf is not None
    assert len(wf.nodes) == 3
    sqn = get_serializable(serialization_settings, square)
    assert sqn.container.image == "alpine"
    sumn = get_serializable(serialization_settings, sum)
    assert sumn.container.image == "alpine"


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

    default_img = Image(name="default", fqn="test", tag="tag")
    serialization_settings = context_manager.SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env=None,
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )
    wf = get_serializable(serialization_settings, my_wf)
    assert wf is not None
    assert len(wf.nodes) == 3
    assert wf.nodes[1].branch_node is not None
    assert wf.nodes[2].branch_node is not None


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

    default_img = Image(name="default", fqn="test", tag="tag")
    serialization_settings = context_manager.SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env=None,
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )
    wf = get_serializable(serialization_settings, my_wf)
    assert wf is not None
    assert len(wf.nodes[0].inputs) == 1
    assert wf.nodes[0].inputs[0].var == ".a"
    assert wf.nodes[0] is not None


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
    serialization_settings = context_manager.SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env=None,
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )
    wf = get_serializable(serialization_settings, my_wf)
    assert wf is not None
    assert len(wf.nodes[0].inputs) == 1
    assert wf.nodes[0].inputs[0].var == ".a"


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
    serialization_settings = context_manager.SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env=None,
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )
    wf = get_serializable(serialization_settings, my_wf)
    assert wf is not None
    assert wf.nodes[1].inputs[0].var == "n0.t1_int_output"


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
    serialization_settings = context_manager.SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env=None,
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )
    wf = get_serializable(serialization_settings, my_wf)
    assert wf is not None
    assert len(wf.nodes) == 2
    assert wf.nodes[1].branch_node is not None


def test_serialization_images():
    @task(container_image="{{.image.xyz.fqn}}:{{.image.default.version}}")
    def t1(a: int) -> int:
        return a

    @task(container_image="{{.image.default.fqn}}:{{.image.default.version}}")
    def t2():
        pass

    @task
    def t3():
        pass

    @task(container_image="docker.io/org/myimage:latest")
    def t4():
        pass

    @task(container_image="docker.io/org/myimage:{{.image.default.version}}")
    def t5(a: int) -> int:
        return a

    os.environ["FLYTE_INTERNAL_IMAGE"] = "docker.io/default:version"
    set_flyte_config_file(os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs/images.config"))
    rs = context_manager.SerializationSettings(
        project="project", domain="domain", version="version", env=None, image_config=get_image_config(),
    )
    t1_ser = get_serializable(rs, t1)
    assert t1_ser.container.image == "docker.io/xyz:version"
    t1_ser.to_flyte_idl()

    t2_ser = get_serializable(rs, t2)
    assert t2_ser.container.image == "docker.io/default:version"

    t3_ser = get_serializable(rs, t3)
    assert t3_ser.container.image == "docker.io/default:version"

    t4_ser = get_serializable(rs, t4)
    assert t4_ser.container.image == "docker.io/org/myimage:latest"

    t5_ser = get_serializable(rs, t5)
    assert t5_ser.container.image == "docker.io/org/myimage:version"


def test_serialization_types():
    @task(cache=True, cache_version="1.0.0")
    def squared(value: int) -> typing.List[typing.Dict[str, int]]:
        return [
            {"squared_value": value ** 2},
        ]

    @workflow
    def compute_square_wf(input_integer: int) -> typing.List[typing.Dict[str, int]]:
        compute_square_result = squared(value=input_integer)
        return compute_square_result

    default_img = Image(name="default", fqn="test", tag="tag")
    serialization_settings = context_manager.SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env=None,
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
    )
    wf = get_serializable(serialization_settings, compute_square_wf)
    assert wf.interface.outputs["o0"].type.collection_type.map_value_type.simple == SimpleType.INTEGER
    ser = get_serializable(serialization_settings, squared)
    assert ser.interface.outputs["o0"].type.collection_type.map_value_type.simple == SimpleType.INTEGER
