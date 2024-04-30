import os

import pytest
from hypothesis import settings

from flytekit.image_spec.image_spec import ImageSpecBuilder


class MockImageSpecBuilder(ImageSpecBuilder):
    def build_image(self, img):
        print("Building an image...")


@pytest.fixture()
def mock_image_spec_builder():
    return MockImageSpecBuilder()


settings.register_profile("ci", max_examples=5, deadline=100_000)
settings.register_profile("dev", max_examples=10, deadline=10_000)

settings.load_profile(os.getenv("FLYTEKIT_HYPOTHESIS_PROFILE", "dev"))
