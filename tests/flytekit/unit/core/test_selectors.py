from collections import OrderedDict

from flyteidl.core.tasks_pb2 import Selector

from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.data_persistence import flyte_tmp_dir
from flytekit.core.selectors import GPUDeviceA100, GPUPartition2G10GB
from flytekit.core.task import task
from flytekit.tools.translator import get_serializable

serialization_settings = SerializationSettings(
    project="proj",
    domain="dom",
    version="123",
    image_config=ImageConfig(Image(name="name", fqn="asdf/fdsa", tag="123")),
    env={},
)


def test_selectors():
    @task(selectors=[GPUDeviceA100, GPUPartition2G10GB])
    def needs_a100(a: int):
        ctx = FlyteContextManager.current_context()
        wf_params = ctx.user_space_params
        assert str(wf_params.execution_id) == "ex:local:local:local"
        assert flyte_tmp_dir in wf_params.raw_output_prefix

    ts = get_serializable(OrderedDict(), serialization_settings, needs_a100)
    assert len(ts.template.metadata.selectors) == 2
    s = ts.template.metadata.selectors[0]
    assert isinstance(s, Selector)
    assert s.HasField("gpu_device")
    assert s.gpu_device == "nvidia-tesla-a100"
