from flytekit.annotated import context_manager
from flytekit.annotated.context_manager import ExecutionState, Image, ImageConfig
from flytekit.annotated.node_creation import create_node
from flytekit.annotated.task import task
from flytekit.annotated.workflow import workflow


def test_normal_task():
    @task
    def t1(a: str) -> str:
        return a + " world"

    @workflow
    def my_wf(a: str) -> str:
        t1_node = create_node(t1, a=a)
        return t1_node.out_0

    r = my_wf(a="hello")
    assert r == "hello world"

    with context_manager.FlyteContext.current_context().new_registration_settings(
        registration_settings=context_manager.RegistrationSettings(
            project="test_proj",
            domain="test_domain",
            version="abc",
            image_config=ImageConfig(Image(name="name", fqn="image", tag="name")),
            env={},
        )
    ):
        sdk_wf = my_wf.get_registerable_entity()
        assert len(sdk_wf.nodes) == 1
        assert len(sdk_wf.outputs) == 1

    @task
    def t2():
        ...

    @task
    def t3():
        ...

    @workflow
    def empty_wf():
        t2_node = create_node(t2)
        t3_node = create_node(t3)
        t2_node.depends_on(t3_node)

    with context_manager.FlyteContext.current_context().new_registration_settings(
        registration_settings=context_manager.RegistrationSettings(
            project="test_proj",
            domain="test_domain",
            version="abc",
            image_config=ImageConfig(Image(name="name", fqn="image", tag="name")),
            env={},
        )
    ):
        sdk_wf = empty_wf.get_registerable_entity()
        assert sdk_wf.nodes[0].upstream_node_ids[0] == "node-1"
        assert sdk_wf.nodes[0].id == "node-0"
