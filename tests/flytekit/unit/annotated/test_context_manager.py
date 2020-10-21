from flytekit.annotated.context_manager import CompilationState, FlyteContext


class SampleTestClass(object):
    def __init__(self, value):
        self.value = value


def test_levels():
    s = SampleTestClass(value=1)
    with FlyteContext(flyte_client=s) as ctx:
        assert ctx.flyte_client.value == 1
        with FlyteContext(flyte_client=SampleTestClass(value=2)) as ctx:
            assert ctx.flyte_client.value == 2

        with FlyteContext(compilation_state=CompilationState()) as ctx:
            assert ctx.flyte_client.value == 1


def test_default():
    ctx = FlyteContext.current_context()
    assert ctx.file_access is not None
