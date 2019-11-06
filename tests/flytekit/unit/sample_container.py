import six
import flytekit
from flytekit.common import sdk_bases
from flytekit.common.tasks import task as _tasks
from flytekit.common.interface import TypedInterface
from flytekit.models.interface import Variable
from flytekit.sdk.types import Types

from flytekit.models.literals import RetryStrategy
from flytekit.models.task import RuntimeMetadata, TaskMetadata, Container, Resources
from flytekit.sdk.workflow import workflow_class, Input

class ManualContainerTask(
    six.with_metaclass(sdk_bases.ExtendedSdkType, _tasks.SdkTask)):

    def __init__(self, metadata, interface, container):
        super(ManualContainerTask, self).__init__(
            type='manual-python-task',
            # if type isn't recognized, propeller will use the default plugin (should be container)
            metadata=metadata,
            interface=interface,
            custom={},
            container=container,
        )


task_metadata = TaskMetadata(
    runtime=RuntimeMetadata(
        type=RuntimeMetadata.RuntimeType.FLYTE_SDK,
        version=flytekit.__version__,
        flavor="python",
    ),
    retries=RetryStrategy(3),
    discoverable=False,
)
task_interface = TypedInterface(inputs={
    "my_int": Variable(Types.Integer.to_flyte_literal_type(), "some integer")
})

container_task = ManualContainerTask(
    metadata=task_metadata,
    interface=task_interface,
    container=Container(image='docker.io/lyft/your_image:abc123',
                        command=["/bin/bash", "-c"],
                        args=["{{.inputs.my_int}}"],
                        resources=Resources(requests=[
                            Resources.ResourceEntry(
                                Resources.ResourceName.MEMORY,
                                '128G'),
                            Resources.ResourceEntry(
                                Resources.ResourceName.CPU, '48')
                        ],
                            limits=[]),
                        env={},
                        config={}))

# you can then use container_task in a workflow
@workflow_class
class WFlow(object):
    input_int = Input(Types.Integer)
    task_instance = container_task(my_int=input_int)
