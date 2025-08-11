from flytekit.image_spec.image_spec import ImageSpec, ImageSpecBuilder


class NoOpBuilder(ImageSpecBuilder):
    """Noop image builder."""

    builder_type = "noop"

    def should_build(self, image_spec: ImageSpec) -> bool:
        """
        The build_image function of NoOpBuilder uses the image_spec name as defined by the user without
        checking whether the image exists in the Docker registry. Therefore, the should_build function
        should always return True to trigger the build_image function.

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
