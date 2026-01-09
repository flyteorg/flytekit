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


def test_noop_builder_should_build():
    """Test that NoOpBuilder.should_build always returns True."""
    builder = NoOpBuilder()

    # Test with different image specs to ensure should_build always returns True
    test_cases = [
        ImageSpec(base_image="localhost:30000/flytekit"),
        ImageSpec(base_image="python:3.9"),
        ImageSpec(base_image="custom/image:latest"),
    ]

    for image_spec in test_cases:
        result = builder.should_build(image_spec)
        assert result is True, f"should_build should return True for {image_spec.base_image}"
