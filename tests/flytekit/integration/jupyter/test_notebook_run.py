from datetime import timedelta
import pathlib
import os
import pytest
from jupyter_client.manager import KernelManager, BlockingKernelClient

from flytekit.configuration import Config
from flytekit.remote import FlyteRemote
import ipykernel.kernelspec


CONFIG = os.environ.get("FLYTECTL_CONFIG", str(pathlib.Path.home() / ".flyte" / "config-sandbox.yaml"))
# Run `make build-dev` to build and push the image to the local registry.
IMAGE = os.environ.get("FLYTEKIT_IMAGE", "localhost:30000/flytekit:dev")
PROJECT = "flytesnacks"
DOMAIN = "development"
VERSION = f"v{os.getpid()}"
KERNEL_NAME = "python3-flytekit-integration"


@pytest.fixture(scope="module", autouse=True)
def install_kernel():
    ipykernel.kernelspec.install(user=True, kernel_name=KERNEL_NAME)


@pytest.fixture
def jupyter_kernel():
    km = KernelManager(kernel_name=KERNEL_NAME)
    km.start_kernel()
    kc = km.client()
    kc.start_channels()
    kc.wait_for_ready()
    yield kc
    kc.stop_channels()
    km.shutdown_kernel()


def execute_code_in_kernel(kc: BlockingKernelClient, code: str):
    kc.execute(code)
    reply = kc.get_shell_msg(timeout=5)
    if reply['content']['status'] == 'error':
        raise RuntimeError(f"Error executing code: {reply['content']}")

    output = []
    while True:
        msg = kc.get_iopub_msg(timeout=5)
        if msg['msg_type'] == 'error':
            raise RuntimeError(f"Error in execution: {msg['content']}")
        elif msg['msg_type'] == 'stream':  # print(...) streams the output out
            output.append(msg['content']['text'].strip())
        if msg['msg_type'] == 'status' and msg['content']['execution_state'] == 'idle':
            break  # nothing is running anymore so we break

    return output


NOTEBOOK_CODE = f"""
from flytekit import task, workflow
from flytekit.configuration import Config
from flytekit.remote import FlyteRemote

remote = FlyteRemote(
    Config.auto("{CONFIG}"),
    default_project="{PROJECT}",
    default_domain="{DOMAIN}",
    interactive_mode_enabled=True,
)

@task(container_image="{IMAGE}")
def hello(name: str) -> str:
    return f"Hello {{name}}"

@task(container_image="{IMAGE}")
def world(pre: str) -> str:
    return f"{{pre}}, Welcome to the world!"

@workflow
def wf(name: str) -> str:
    return world(pre=hello(name=name))

out = remote.execute(wf, inputs={{"name": "flytekit"}}, version="{VERSION}")
print(out.id.name)
"""


def test_jupyter_code_execution(jupyter_kernel):
    output = execute_code_in_kernel(jupyter_kernel, NOTEBOOK_CODE)
    assert len(output) == 1

    remote = FlyteRemote(Config.auto(config_file=CONFIG), PROJECT, DOMAIN)
    execution_id = output[0]
    execution = remote.fetch_execution(name=execution_id)
    execution = remote.wait(execution, sync_nodes=True, poll_interval=timedelta(seconds=5))

    assert execution.outputs["o0"] == "Hello flytekit, Welcome to the world!"
