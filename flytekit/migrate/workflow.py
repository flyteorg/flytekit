import flytekit

from flyte import Image, Resources, TaskEnvironment

env = TaskEnvironment(
    name="flytekit",
    resources=Resources(cpu=0.8, memory="800Mi"),
    image=Image.from_debian_base().with_apt_packages("vim").with_pip_packages("flytekit", "pandas"),
)

# TODO: Build subtask's image

flytekit.workflow = env.task
