from flytekit.annotated import context_manager
from flytekit.annotated.context_manager import FlyteContext
from flytekit.annotated.task import ContainerTask, kwtypes, metadata
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
    with ctx.current_context().new_registration_settings(
            registration_settings=registration_settings
    ):
        tk1 = square.get_registerable_entity()
        tk2 = sum.get_registerable_entity()
        wf = raw_container_wf.get_registerable_entity()
        assert wf is not None
        print(wf)
