import pytest

from flytekit.configuration import Config, ImageConfig, SerializationSettings
from flytekit.remote.remote import FlyteRemote

from .resources import hello_wf

###
# THESE TESTS ARE NOT RUN IN CI. THEY ARE HERE TO MAKE LOCAL TESTING EASIER.
# Update this to use these tests
SANDBOX_CONFIG_FILE = "/Users/ytong/.flyte/local_sandbox"
###


@pytest.mark.sandbox_test
def test_fetch_one_wf():
    rr = FlyteRemote(
        Config.auto(config_file=SANDBOX_CONFIG_FILE),
        default_project="flytesnacks",
        default_domain="development",
    )
    wf = rr.fetch_workflow(name="core.flyte_basics.files.rotate_one_workflow", version="v0.3.56")
    # rr.recent_executions(wf)
    print(str(wf))


@pytest.mark.sandbox_test
def test_get_parent_wf_run():
    rr = FlyteRemote(
        Config.auto(config_file=SANDBOX_CONFIG_FILE),
        default_project="flytesnacks",
        default_domain="development",
    )
    we = rr.fetch_workflow_execution(name="vudmhuxb9b")
    rr.sync_workflow_execution(we, sync_nodes=True)
    print(we)


@pytest.mark.sandbox_test
def test_get_merge_sort_run():
    rr = FlyteRemote(
        Config.auto(config_file=SANDBOX_CONFIG_FILE),
        default_project="flytesnacks",
        default_domain="development",
    )
    we = rr.fetch_workflow_execution(name="djdo2l2s0s")
    rr.sync_workflow_execution(we, sync_nodes=True)
    print(we)


@pytest.mark.sandbox_test
def test_fetch_merge_sort():
    rr = FlyteRemote(
        Config.auto(config_file=SANDBOX_CONFIG_FILE),
        default_project="flytesnacks",
        default_domain="development",
    )
    wf = rr.fetch_workflow(name="core.control_flow.run_merge_sort.merge_sort", version="v0.3.56")
    print(wf)


@pytest.mark.sandbox_test
def test_register_a_hello_world_wf():
    rr = FlyteRemote(
        Config.auto(config_file=SANDBOX_CONFIG_FILE),
        default_project="flytesnacks",
        default_domain="development",
    )
    ic = ImageConfig.auto(img_name="docker.io/def:latest")
    ver = "test_unit_7"

    ss = SerializationSettings(ic, project="flytesnacks", domain="development", version=ver)
    rr.register_workflow(hello_wf, serialization_settings=ss)

    fetched_wf = rr.fetch_workflow(name=hello_wf.name, version=ver)

    rr.execute(fetched_wf, inputs={"a": 5})


@pytest.mark.sandbox_test
def test_run_directly_hello_world_wf():
    rr = FlyteRemote(
        Config.auto(config_file=SANDBOX_CONFIG_FILE),
        default_project="flytesnacks",
        default_domain="development",
    )
    ic = ImageConfig.auto(img_name="docker.io/def:latest")
    ver = "test_unit_9"

    ss = SerializationSettings(ic, project="flytesnacks", domain="development", version=ver)
    rr.execute(hello_wf, inputs={"a": 5}, serialization_settings=ss)
