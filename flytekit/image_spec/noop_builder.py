from flytekit.image_spec.image_spec import ImageSpec, ImageSpecBuilder


class NoOpBuilder(ImageSpecBuilder):
    """Noop image builder."""

    builder_type = "noop"

    def build_image(self, image_spec: ImageSpec) -> str:
        if not isinstance(image_spec.base_image, str):
            msg = "base_image must be a string to use the noop image builder"
            raise ValueError(msg)

        import click

        click.secho(f"Using image: {image_spec.base_image}", fg="blue")
        return image_spec.base_image
