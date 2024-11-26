from collections import OrderedDict

import mock
import os
import pytest
from flytekitplugins.appliedflyteinteractive import JupyterConfig

from flytekit import task, workflow
from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.core.context_manager import ExecutionState
from flytekit.tools.translator import get_serializable_task

POD_NAME = "execution-id-n0"


@pytest.fixture
def mock_local_execution():
    with mock.patch.object(
        ExecutionState, "is_local_execution", return_value=True
    ) as mock_func:
        yield mock_func


@pytest.fixture
def mock_remote_execution():
    os.environ["POD_NAME"] = POD_NAME
    with mock.patch.object(
        ExecutionState, "is_local_execution", return_value=False
    ) as mock_func:
        yield mock_func

    os.environ.pop("POD_NAME")


@pytest.fixture
def jupyter_patches():
    with mock.patch("multiprocessing.Process") as mock_process, mock.patch(
        "flytekitplugins.appliedflyteinteractive.jupyter_lib.task.write_example_notebook"
    ) as mock_write_example_notebook, mock.patch(
        "flytekitplugins.appliedflyteinteractive.jupyter_lib.task._must_install_jupyter_dependencies"
    ) as mock_must_install_jupyter_dependencies, mock.patch(
        "flytekitplugins.appliedflyteinteractive.jupyter_lib.task.JupyterK8sResources"
    ) as mock_jupyter_k8s_resource:
        mock_jupyter_k8s_resource_instance = mock_jupyter_k8s_resource.return_value

        yield (
            mock_process,
            mock_write_example_notebook,
            mock_must_install_jupyter_dependencies,
            mock_jupyter_k8s_resource_instance.make_resources,
            mock_jupyter_k8s_resource_instance.delete_resources,
        )


def test_jupyter_remote_execution(jupyter_patches, mock_remote_execution):
    (
        mock_process,
        mock_write_example_notebook,
        mock_must_install_jupyter_dependencies,
        mock_k8s_make_resources,
        mock_k8s_delete_resources,
    ) = jupyter_patches

    @task(task_config=JupyterConfig())
    def t():
        return

    @workflow
    def wf():
        t()

    wf()
    mock_process.assert_called_once()
    mock_write_example_notebook.assert_called_once()
    mock_must_install_jupyter_dependencies.assert_called_once()
    mock_k8s_make_resources.assert_called_once()
    mock_k8s_delete_resources.assert_called_once()


def test_jupyter_remote_execution_but_disable(jupyter_patches, mock_remote_execution):
    (
        mock_process,
        mock_write_example_notebook,
        mock_must_install_jupyter_dependencies,
        mock_k8s_make_resources,
        mock_k8s_delete_resources,
    ) = jupyter_patches

    @task(task_config=JupyterConfig(enable=False))
    def t():
        return

    @workflow
    def wf():
        t()

    wf()
    mock_process.assert_not_called()
    mock_write_example_notebook.assert_not_called()
    mock_must_install_jupyter_dependencies.assert_not_called()
    mock_k8s_make_resources.assert_not_called()
    mock_k8s_delete_resources.assert_not_called()


def test_jupyter_local_execution(jupyter_patches, mock_local_execution):
    (
        mock_process,
        mock_write_example_notebook,
        mock_must_install_jupyter_dependencies,
        mock_k8s_make_resources,
        mock_k8s_delete_resources,
    ) = jupyter_patches

    @task(task_config=JupyterConfig())
    def t():
        return

    @workflow
    def wf():
        t()

    wf()
    mock_process.assert_not_called()
    mock_write_example_notebook.assert_not_called()
    mock_must_install_jupyter_dependencies.assert_not_called()
    mock_k8s_make_resources.assert_not_called()
    mock_k8s_delete_resources.assert_not_called()


def test_jupyter_able_to_run_task(jupyter_patches, mock_remote_execution):
    @task(task_config=JupyterConfig())
    def t(a: int, b: int) -> int:
        return a + b

    @workflow
    def wf(a: int, b: int) -> int:
        out = t(a=a, b=b)
        return out

    res = wf(a=7, b=8)
    assert res == 15


def test_jupyter_pod_template():
    @task(task_config=JupyterConfig(enable=False))
    def t():
        return

    assert any(
        env.name == "POD_NAME" for env in t.pod_template.pod_spec.containers[0].env
    )
    assert any(
        env.name == "POD_NAMESPACE" for env in t.pod_template.pod_spec.containers[0].env
    )
