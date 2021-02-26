from flytekit.common.translator import get_serializable
from flytekit.core import context_manager
from flytekit.core.context_manager import Image, ImageConfig
from flytekit.pattern.builder import PickleBuilder


def get_hello(a: int):
    if a == 5:

        def hello() -> int:
            print("hello")
            return 5

        return hello

    def hello() -> int:
        print(f"Hello, A was {a}")
        return 42

    return hello


if __name__ == "__main__":
    # Using the pickling resolver
    # What the user would write, not qualified by a main conditional
    # At serialization time, this file gets picked up.

    wf = PickleBuilder().set_do_fn(get_hello(42)).build()

    # Below this is for testing only. User would not write this.
    serialization_settings = context_manager.SerializationSettings(
        project="proj",
        domain="dom",
        version="123",
        image_config=ImageConfig(Image(name="name", fqn="asdf/fdsa", tag="123")),
        env={},
    )

    # Serializing the workflow
    idl_wf = get_serializable(serialization_settings, wf)
    print(idl_wf)

    # Serializing the task, because there's only one task and one workflow in this main, and the task gets created
    # first, it'll be in index 0.
    from flytekit.core.context_manager import FlyteEntities

    task_obj = FlyteEntities.entities[0]
    idl_task = get_serializable(serialization_settings, task_obj)
    print(idl_task)
