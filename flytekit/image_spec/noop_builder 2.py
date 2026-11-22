from flytekit.image_spec.image_spec import ImageSpec, ImageSpecBuilder


class NoOpBuilder(ImageSpecBuilder):
    """Noop image builder."""

    builder_type = "noop"

    def should_build(self, image_spec: ImageSpec) -> bool:
        """
        The build_image function of NoOpBuilder does not actually build a Docker image.
        Since no Docker build process occurs, we do not need to check for Docker daemon
        or existing images. Therefore, should_build should always return True.

        Args:
            image_spec (ImageSpec): Image specification

        Returns:
            bool: Always returns True
        """
        return True

    def build_image(self, image_spec: ImageSpec) -> str:
        if not isinstance(image_spec.base_image, str):
            msg = "base_image must be a string to use the noop image builder"
            raise ValueError(msg)

        import click

        click.secho(f"Using image: {image_spec.base_image}", fg="blue")
        return image_spec.base_image
