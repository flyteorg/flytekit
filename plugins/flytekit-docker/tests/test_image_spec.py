from pathlib import Path
from unittest import mock

from flytekitplugins.docker.image_builder import DockerfileImageSpecBuilder

from flytekit.image_spec.image_spec import ImageSpec
# from flytekit import task, workflow


@mock.patch("flytekitplugins.docker.image_builder.DockerfileImageSpecBuilder.execute_command")
@mock.patch("flytekit.image_spec.image_spec.calculate_hash_from_image_spec")
def test_image_spec(calculate_hash_from_image_spec, execute_command, test_data_dir: Path):
    dockerfile_path = test_data_dir / "Dockerfile.biopython"
    image_spec = ImageSpec(
        name="test-biopython",
        builder="docker",
        dockerfile=str(dockerfile_path),
        source_root=str(dockerfile_path.parent),
        docker_build_extra_args=["--build-arg", "HWARRG=WORLD"],
    )
    # We mock this so that the temporary directory changes don't affect the hash
    calculate_hash_from_image_spec.return_value = "qI_WTTWzh4qTZS3cAVFlXQ"

    DockerfileImageSpecBuilder().build_image(image_spec)
    [submitted_command] = execute_command.call_args.args

    platform_index = submitted_command.index("--platform")
    platform_set_index = submitted_command.index("linux/amd64")
    assert platform_index + 1 == platform_set_index
    assert str(dockerfile_path) in submitted_command

    # Extra checks?
    # Is remote used?
    # is container used?
    #
    # image_name = image_spec.image_name()

    # @task(container_image=image_spec)
    # def bio_version_task() -> str:
    #     import Bio
    #     return Bio.__version__
    #
    # @workflow
    # def bio_version_wf() -> str:
    #     return bio_version_task()
    #
    # ret = bio_version_wf()
    # print(ret)
    # assert ret == "1.81"
    # assert False
