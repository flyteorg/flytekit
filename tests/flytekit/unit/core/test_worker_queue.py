from flytekit.remote.remote import FlyteRemote
from flytekit.core.worker_queue import Controller
from flytekit.configuration import ImageConfig, LocalConfig, SerializationSettings


def test_controller():
    remote = FlyteRemote.for_sandbox()
    ss = SerializationSettings(
        image_config=ImageConfig.auto_default_image(),
    )
    controller = Controller(remote, ss)
