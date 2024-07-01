import os

import pytest

from flytekit.image_spec import ImageSpec
from flytekit.image_spec.default_builder import DefaultImageBuilder, create_docker_context


def test_create_docker_context(tmp_path):
    docker_context_path = tmp_path / "builder_root"
    docker_context_path.mkdir()

    source_root = tmp_path / "other_files"
    source_root.mkdir()
    hello_world_path = source_root / "hello_world.txt"
    hello_world_path.write_text("hello")

    other_requirements_path = tmp_path / "requirements.txt"
    other_requirements_path.write_text("threadpoolctl\n")

    image_spec = ImageSpec(
        name="FLYTEKIT",
        python_version="3.12",
        env={"MY_ENV": "MY_VALUE"},
        apt_packages=["curl"],
        conda_packages=["scipy==1.13.0", "numpy"],
        packages=["pandas==2.2.1"],
        requirements=os.fspath(other_requirements_path),
        source_root=os.fspath(source_root),
        commands=["mkdir my_dir"],
    )

    create_docker_context(image_spec, docker_context_path)

    dockerfile_path = docker_context_path / "Dockerfile"
    assert dockerfile_path.exists()
    dockerfile_content = dockerfile_path.read_text()

    assert "curl" in dockerfile_content
    assert "scipy==1.13.0 numpy" in dockerfile_content
    assert "python=3.12" in dockerfile_content
    assert "--requirement requirements_uv.txt" in dockerfile_content
    assert "COPY --chown=flytekit ./src /root" in dockerfile_content
    assert "RUN mkdir my_dir" in dockerfile_content

    requirements_path = docker_context_path / "requirements_uv.txt"
    assert requirements_path.exists()

    requirements_content = requirements_path.read_text()
    assert "pandas==2.2.1" in requirements_content
    assert "threadpoolctl" in requirements_content

    tmp_hello_world = docker_context_path / "src" / "hello_world.txt"
    assert tmp_hello_world.exists()
    assert tmp_hello_world.read_text() == "hello"


def test_create_docker_context_with_git_subfolder(tmp_path):
    # uv's pip install errors with git and subdirectory
    # In this case, we go back to pip instead
    docker_context_path = tmp_path / "builder_root"
    docker_context_path.mkdir()

    image_spec = ImageSpec(
        name="FLYTEKIT",
        python_version="3.12",
        apt_packages=["git"],
        packages=["git+https://github.com/flyteorg/flytekit.git@master#subdirectory=plugins/flytekit-wandb"],
    )

    create_docker_context(image_spec, docker_context_path)

    dockerfile_path = docker_context_path / "Dockerfile"
    assert dockerfile_path.exists()
    dockerfile_content = dockerfile_path.read_text()

    assert "--requirement requirements_pip.txt" in dockerfile_content
    requirements_path = docker_context_path / "requirements_pip.txt"
    assert requirements_path.exists()


def test_create_docker_context_cuda(tmp_path):
    docker_context_path = tmp_path / "builder_root"
    docker_context_path.mkdir()

    image_spec = ImageSpec(cuda="12.4.1", cudnn="8")

    msg = "cuda and cudnn do not need to be specified. If you are installed"

    with pytest.raises(ValueError, match=msg):
        create_docker_context(image_spec, docker_context_path)


@pytest.mark.skipif(
    os.environ.get("_FLYTEKIT_TEST_DEFAULT_BUILDER", "0") == "0",
    reason="Set _FLYTEKIT_TEST_DEFAULT_BUILDER=1 to run this test",
)
def test_build(tmp_path):
    docker_context_path = tmp_path / "builder_root"
    docker_context_path.mkdir()

    source_root = tmp_path / "other_files"
    source_root.mkdir()
    hello_world_path = source_root / "hello_world.txt"
    hello_world_path.write_text("hello")

    other_requirements_path = tmp_path / "requirements.txt"
    other_requirements_path.write_text("threadpoolctl\n")

    image_spec = ImageSpec(
        name="FLYTEKIT",
        python_version="3.12",
        env={"MY_ENV": "MY_VALUE"},
        apt_packages=["curl"],
        conda_packages=["scipy==1.13.0", "numpy"],
        packages=["pandas==2.2.1"],
        requirements=os.fspath(other_requirements_path),
        source_root=os.fspath(source_root),
        commands=["mkdir my_dir"],
    )

    builder = DefaultImageBuilder()

    builder.build_image(image_spec)
