from pathlib import Path

from flytekit import task
from flytekit.image_spec import ImageSpec

PARENT_DIR = Path(__file__).parent
DOCKERFILE_DIR = PARENT_DIR / "dockerfiles"


biopython_img = ImageSpec(
    name="flytekit-biopython",
    builder="docker",
    dockerfile=str(DOCKERFILE_DIR/ "Dockerfile.biopython"),
    source_root=str(DOCKERFILE_DIR),
    registry="dancyrusbio",
)


@task(container_image=biopython_img)
def t1() -> str:
    import Bio

    return Bio.__version__
