import subprocess

import click

from flytekit.image_spec.image_spec import ImageBuildEngine, ImageSpec, ImageSpecBuilder


class DockerfileImageSpecBuilder(ImageSpecBuilder):
    """
    This class is used to build a docker image using docker
    """

    def execute_command(self, command: list[str]) -> None:
        print("running maybe?", " ".join(command))
        click.secho(f"Run command: {' '.join(command)} ", fg="blue")
        p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        for line in iter(p.stdout.readline, ""):
            if p.poll() is not None:
                break
            line_out = line.decode().strip()
            if line.decode().strip() != "":
                click.secho(line_out, fg="blue")
                print(line_out)

        if p.returncode != 0:
            _, stderr = p.communicate()
            raise Exception(f"failed to run command {command} with error {stderr}")

    def build_image(self, image_spec: ImageSpec):
        if image_spec.dockerfile is None:
            raise RuntimeError("Image spec dockerfile cannot be None")
        command = [
            "docker",
            "buildx",
            "build",
            "-f",
            image_spec.dockerfile,
            "--platform",
            image_spec.platform,
            image_spec.source_root,
        ] + image_spec.docker_build_extra_args
        output_image_params = f"type=image,name={image_spec.image_name()}"
        if image_spec.registry:
            output_image_params = f"{output_image_params},push=true"
        command += [
            "--output",
            output_image_params,
        ]
        self.execute_command(command)


ImageBuildEngine.register("docker", DockerfileImageSpecBuilder())
