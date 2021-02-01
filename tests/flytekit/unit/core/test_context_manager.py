from flytekit.core.context_manager import CompilationState, FlyteContext, look_up_image_info


class SampleTestClass(object):
    def __init__(self, value):
        self.value = value


def test_levels():
    s = SampleTestClass(value=1)
    with FlyteContext(flyte_client=s) as ctx:
        assert ctx.flyte_client.value == 1
        with FlyteContext(flyte_client=SampleTestClass(value=2)) as ctx:
            assert ctx.flyte_client.value == 2

        with FlyteContext(compilation_state=CompilationState(prefix="")) as ctx:
            assert ctx.flyte_client.value == 1


def test_default():
    ctx = FlyteContext.current_context()
    assert ctx.file_access is not None


def test_look_up_image_info():
    img = look_up_image_info(name="x", tag="docker.io/xyz", optional_tag=True)
    assert img.name == "x"
    assert img.tag is None
    assert img.fqn == "docker.io/xyz"

    img = look_up_image_info(name="x", tag="docker.io/xyz:latest", optional_tag=True)
    assert img.name == "x"
    assert img.tag == "latest"
    assert img.fqn == "docker.io/xyz"

    img = look_up_image_info(name="x", tag="docker.io/xyz:latest", optional_tag=False)
    assert img.name == "x"
    assert img.tag == "latest"
    assert img.fqn == "docker.io/xyz"
