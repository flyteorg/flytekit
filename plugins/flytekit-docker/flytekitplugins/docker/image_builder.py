import os
import subprocess
import tempfile

import click

from flytekit.image_spec.image_spec import _F_IMG_ID, ImageBuildEngine, ImageSpec, ImageSpecBuilder


class DockerfileImageSpecBuilder(ImageSpecBuilder):
    """
    This class is used to build a docker image using docker
    """

    def execute_command(self, command: list[str]) -> None:
        click.secho(f"Run command: {' '.join(command)} ", fg="blue")
        p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        for line in iter(p.stdout.readline, ""):
            if p.poll() is not None:
                break
            line_out = line.decode().strip()
            if line.decode().strip() != "":
                click.secho(line_out, fg="blue")

        if p.returncode != 0:
            _, stderr = p.communicate()
            raise Exception(f"failed to run command {command} with error {stderr}")

    def build_image(self, image_spec: ImageSpec):
        if image_spec.dockerfile is None:
            raise RuntimeError("Image spec dockerfile cannot be None")
        if image_spec.platform is None:
            raise RuntimeError("Image spec platform cannot be None")
        if image_spec.source_root is None:
            raise RuntimeError("Image spec source_root cannot be None")
        env_image_id = os.environ.get(_F_IMG_ID)
        if env_image_id:
            click.secho(f"Skipping nested build of {image_spec.name}", fg="blue")

            def horrible_horrible_hack() -> str:
                return env_image_id

            image_spec.image_name = horrible_horrible_hack
            return

        # Inject _F_IMG_ID to stop recursive builds
        injected_env = {"PYTHONPATH": "/root", _F_IMG_ID: image_spec.image_name()}
        if image_spec.env is not None:
            injected_env.update(image_spec.env)

        with open(image_spec.dockerfile) as fh:
            starting_text = fh.read()
        env_text = "\n".join(f"ENV {k}={v}" for k, v in injected_env.items())
        temporary_dockerfile_text = f"{starting_text}\n{env_text}"

        with tempfile.NamedTemporaryFile("w") as fh:
            fh.write(temporary_dockerfile_text)
            fh.flush()
            command = [
                "docker",
                "buildx",
                "build",
                "-f",
                fh.name,
                "--platform",
                image_spec.platform,
                image_spec.source_root,
            ] + image_spec.docker_build_extra_args
            output_image_params = f"type=image,name={image_spec.image_name()}"
            if image_spec.registry:
                output_image_params = f"{output_image_params},push=true"
            if image_spec.buildkit_build_extra_output:
                output_image_params = f"{output_image_params},{image_spec.buildkit_build_extra_output}"
            command += [
                "--output",
                output_image_params,
            ]
            self.execute_command(command)


ImageBuildEngine.register("docker", DockerfileImageSpecBuilder())
