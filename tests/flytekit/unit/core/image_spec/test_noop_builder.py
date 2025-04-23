import pytest
from flytekit.image_spec.noop_builder import NoOpBuilder
from flytekit import ImageSpec


def test_noop_builder():
    builder = NoOpBuilder()

    image_spec = ImageSpec(base_image="localhost:30000/flytekit")
    image = builder.build_image(image_spec)
    assert image == "localhost:30000/flytekit"


@pytest.mark.parametrize("base_image", [
    None,
    ImageSpec(base_image="another_none")
])
def test_noop_builder_error(base_image):
    builder = NoOpBuilder()

    msg = "base_image must be a string to use the noop image builder"
    image_spec = ImageSpec(base_image=base_image)
    with pytest.raises(ValueError, match=msg):
        builder.build_image(image_spec)
