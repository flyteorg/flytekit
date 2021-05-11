from flytekit.core.context_manager import ExecutionState, FlyteContext, FlyteContextManager, look_up_image_info


class SampleTestClass(object):
    def __init__(self, value):
        self.value = value


def test_levels():
    ctx = FlyteContextManager.current_context()
    b = ctx.new_builder()
    b.flyte_client = SampleTestClass(value=1)
    with FlyteContextManager.with_context(b) as outer:
        assert outer.flyte_client.value == 1
        b = outer.new_builder()
        b.flyte_client = SampleTestClass(value=2)
        with FlyteContextManager.with_context(b) as ctx:
            assert ctx.flyte_client.value == 2

        with FlyteContextManager.with_context(outer.with_new_compilation_state()) as ctx:
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

    img = look_up_image_info(name="x", tag="localhost:5000/xyz:latest", optional_tag=False)
    assert img.name == "x"
    assert img.tag == "latest"
    assert img.fqn == "localhost:5000/xyz"


def test_additional_context():
    ctx = FlyteContext.current_context()
    with FlyteContextManager.with_context(
        ctx.with_execution_state(
            ctx.new_execution_state().with_params(
                mode=ExecutionState.Mode.TASK_EXECUTION, additional_context={1: "outer", 2: "foo"}
            )
        )
    ) as exec_ctx_outer:
        with FlyteContextManager.with_context(
            ctx.with_execution_state(
                exec_ctx_outer.execution_state.with_params(
                    mode=ExecutionState.Mode.TASK_EXECUTION, additional_context={1: "inner", 3: "baz"}
                )
            )
        ) as exec_ctx_inner:
            assert exec_ctx_inner.execution_state.additional_context == {1: "inner", 2: "foo", 3: "baz"}
