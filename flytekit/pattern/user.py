from flytekit.common.translator import get_serializable
from flytekit.core import context_manager
from flytekit.core.context_manager import Image, ImageConfig

from flytekit.pattern.class_based_resolver import ClassStorageTaskResolver

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
    # Using the class-based task resolver
    # What the user would write, not qualified by a main conditional
    # At serialization time, this file gets picked up.
    MyBuilder()
    MyBuilder.add(get_hello(3))
    MyBuilder.add(get_hello(4))
    MyBuilder.add(get_hello(5))

    # Below this is for testing only. User would not write this.
    serialization_settings = context_manager.SerializationSettings(
        project="proj",
        domain="dom",
        version="123",
        image_config=ImageConfig(Image(name="name", fqn="asdf/fdsa", tag="123")),
        env={},
    )
    all_tasks = MyBuilder.get_all_tasks()
    t = all_tasks[0]
    tt = get_serializable(serialization_settings, t)
    print(tt.container.args)
