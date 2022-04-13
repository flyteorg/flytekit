from flytekit.configuration import ImageConfig
from flytekit.configuration.default_images import DefaultImages, PythonVersion


def test_def():
    img_str = DefaultImages.find_image_for(PythonVersion.PYTHON_3_8)
    print(img_str)


def test_fds():
    x = ImageConfig.auto_default_image()
    print(x)
