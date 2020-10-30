import typing

from flytekit.annotated import context_manager
from flytekit.annotated.condition import conditional
from flytekit.annotated.context_manager import FlyteContext
from flytekit.annotated.task import ContainerTask, kwtypes, metadata, task
from flytekit.annotated.workflow import workflow


def test_serialization():
    square = ContainerTask(
        name="square",
        metadata=metadata(),
        input_data_dir="/var/inputs",
        output_data_dir="/var/outputs",
        inputs=kwtypes(val=int),
        outputs=kwtypes(out=int),
        image="alpine",
        command=["sh", "-c", "echo $(( {{.Inputs.val}} * {{.Inputs.val}} )) | tee /var/outputs/out"],
    )

    sum = ContainerTask(
        name="sum",
        metadata=metadata(),
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

    ctx = FlyteContext.current_context()
    registration_settings = context_manager.RegistrationSettings(
        project="project", domain="domain", version="version", image="image", env=None,
    )
    with ctx.current_context().new_registration_settings(registration_settings=registration_settings):
        wf = raw_container_wf.get_registerable_entity()
        assert wf is not None
        print(wf)


def test_serialization_branch():
    @task
    def t1(a: int) -> typing.NamedTuple("OutputsBC", t1_int_output=int, c=str):
        return a + 2, "world"

    @task
    def t2(a: str) -> str:
        return a

    @workflow
    def my_wf(a: int, b: str) -> (int, str):
        x, y = t1(a=a)
        d = conditional("test1").if_(x == 4).then(t2(a=b)).elif_(x >= 5).then(t2(a=y)).else_().fail(
            "Unable to choose branch")
        f = conditional("test2").if_(d == "hello ").then(t2(a="It is hello")).else_().then(t2(a="Not Hello!"))
        return x, f

    ctx = FlyteContext.current_context()
    registration_settings = context_manager.RegistrationSettings(
        project="project", domain="domain", version="version", image="image", env=None,
    )
    with ctx.current_context().new_registration_settings(registration_settings=registration_settings):
        wf = my_wf.get_registerable_entity()
        assert wf is not None
        print(wf)
