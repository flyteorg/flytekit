import os
from pathlib import Path

from flytekit import dynamic, task
from flytekit.image_spec import ImageSpec

PARENT_DIR = Path(__file__).parent
DOCKERFILE_DIR = PARENT_DIR / "dockerfiles"


biopython_img = ImageSpec(
    name="flytekit-biopython",
    builder="docker",
    dockerfile=str(DOCKERFILE_DIR / "Dockerfile.biopython"),
    source_root=str(DOCKERFILE_DIR),
    registry="dancyrusbio",
    env={"TESTARG": "FOOBAR"},
    docker_build_extra_args=["--build-arg", "HWARG=buildargWORLD"],
)


@task(container_image=biopython_img)
def t1() -> str:
    import Bio

    maybe_world = os.getenv("HELLO", "XXX")
    pushed_env = os.getenv("TESTARG", "YYY")

    return f"{Bio.__version__} -- '{maybe_world}' -- '{pushed_env}'"


# You cannot use workflow, because it uses the base image and the base image
# does not have flytekit-docker installed
@dynamic(container_image=biopython_img)
def wf_t1() -> str:
    return t1()
