import inspect
import multiprocessing
import os
from dataclasses import dataclass
from typing import Callable, Optional

import nbformat as nbf
from flytekitplugins.appliedflyteinteractive.utils import execute_command
from kubernetes import client, config
from kubernetes.dynamic import DynamicClient

from flytekit import PythonFunctionTask
from flytekit.core.context_manager import ExecutionParameters, FlyteContextManager
from flytekit.core.pod_template import PodTemplate
from flytekit.extend import TaskPlugins
from flytekit.loggers import logger

from ..constants import MAX_IDLE_SECONDS
from .jupyter_constants import EXAMPLE_JUPYTER_NOTEBOOK_NAME


class JupyterK8sResources:
    """
    JupyterK8sResources manages the k8s resources responsible for routing requests to
    the jupyter notebook pod. Ideally these operations are done in the flytepropeller.
    """

    MAPPING_KIND = "Mapping"
    MAPPING_API_GROUP = "getambassador.io/v3alpha1"

    def __init__(self, jupyter_path: str, port: Optional[int]):
        config.load_incluster_config()

        raw_client = client.ApiClient()
        self._client = client.CoreV1Api(raw_client)
        self._dynamic_client = DynamicClient(raw_client)
        self._mapping_resource = self._dynamic_client.resources.get(
            api_version=JupyterK8sResources.MAPPING_API_GROUP,
            kind=JupyterK8sResources.MAPPING_KIND,
        )

        self._namespace = os.environ["POD_NAMESPACE"]
        self._pod_name = os.environ["POD_NAME"]
        self._flyte_url = os.environ["FLYTE_URL"]
        self._jupyter_path = jupyter_path
        self._port = port
        self._execution_id = os.environ["FLYTE_INTERNAL_EXECUTION_ID"]

        self._mapping_name = f"jupyter-{self._pod_name}"
        self._headless_service_name = f"jupyter-{self._pod_name}"
        self._headless_service_name_with_port = f"{self._headless_service_name}:{self._port}"

    def make_resources(self):
        self._make_headless_service(self._headless_service_name)
        self._make_mapping_resource(self._mapping_name, self._headless_service_name_with_port)

    def delete_resources(self):
        self._delete_mapping_resource(self._mapping_name)
        self._delete_headless_service(self._headless_service_name)

    def _make_mapping_resource(
        self,
        mapping_name: str,
        headless_service_name_with_port: str,
    ) -> None:
        mapping_body = {
            "apiVersion": JupyterK8sResources.MAPPING_API_GROUP,
            "kind": JupyterK8sResources.MAPPING_KIND,
            "metadata": {
                "name": mapping_name,
                "namespace": self._namespace,
            },
            "spec": {
                "allow_upgrade": ["websocket"],
                "host": self._flyte_url,
                "prefix": self._jupyter_path,
                "rewrite": self._jupyter_path,
                "service": headless_service_name_with_port,
            },
        }
        self._mapping_resource.create(body=mapping_body)

    def _make_headless_service(self, headless_service_name: str):
        # Flyte will attach the unique node id for the execution as a pod label.
        # There is no good way to get pod labels as an environment variable since it is
        # attached at pod schedule time.
        node_id = self._client.read_namespaced_pod(self._pod_name, self._namespace).metadata.labels["node-id"]
        service_port = client.V1ServicePort(name="notebook-port", port=self._port, target_port=self._port)

        self._client.create_namespaced_service(
            namespace=self._namespace,
            body=client.V1Service(
                metadata=client.V1ObjectMeta(
                    name=headless_service_name,
                    namespace=self._namespace,
                ),
                spec=client.V1ServiceSpec(
                    cluster_ip="None",
                    selector={"execution-id": self._execution_id, "node-id": node_id},
                    ports=[service_port],
                ),
            ),
        )

    def _delete_mapping_resource(self, mapping_name: str) -> None:
        self._mapping_resource.delete(name=mapping_name, namespace=self._namespace)

    def _delete_headless_service(self, headless_service_name: str):
        self._client.delete_namespaced_service(name=headless_service_name, namespace=self._namespace)


def _must_get_jupyter_path(path_override: Optional[str], is_local_execution: bool) -> str:
    """
    Get path that will route to the jupyter notebook.

    Args:
        path_override (optional(str)): path to use instead of default
        is_local_execution (bool): true if executed locally.
    """
    if path_override:
        return f"/jupyter/{path_override}"

    if is_local_execution:
        return "/jupyter/local"

    pod_name = os.environ.get("POD_NAME", None)
    if not pod_name:
        raise RuntimeError("POD_NAME needs to be set")

    return f"/jupyter/{pod_name}"


def _must_install_jupyter_dependencies() -> None:
    # Clear out PYTHON_PATH so notebooks are installed into system pip packages.
    # There is a bug in notebook==7.x.x that prevent it from running in a bazel context.
    # https://github.com/bazelbuild/rules_python/issues/63#issuecomment-1934027548.
    execute_command(
        "pip install jupyter==1.0.0 notebook==7.2.1",
        env={},
    )


def write_example_notebook(task_function: Optional[Callable], notebook_dir: str):
    """
    Create an example notebook with markdown and code cells that show instructions to resume task & jupyter task code.

    Args:
        task_function (function): User's task function.
        notebook_dir (str): Local path to write the example notebook to
    """
    nb = nbf.v4.new_notebook()

    first_cell = "### This file is auto-generated by flyteinteractive"
    second_cell = """from flytekit import task
from flytekitplugins.appliedflyteinteractive import jupyter"""
    third_cell = inspect.getsource(task_function)
    fourth_cell = f"{task_function.__name__}()"
    fifth_cell = "### Resume task by shutting down Jupyter: File -> Shut Down"

    nb["cells"] = [
        nbf.v4.new_markdown_cell(first_cell),
        nbf.v4.new_code_cell(second_cell),
        nbf.v4.new_code_cell(third_cell),
        nbf.v4.new_code_cell(fourth_cell),
        nbf.v4.new_markdown_cell(fifth_cell),
    ]
    nbf.write(nb, f"{notebook_dir}/{EXAMPLE_JUPYTER_NOTEBOOK_NAME}")


def exit_handler(
    child_process: multiprocessing.Process,
    task_function,
    args,
    kwargs,
    post_execute: Optional[Callable] = None,
):
    """
    1. Wait for the child process to finish. This happens when the user clicks "Shut Down" in Jupyter
    2. Execute post function, if given.
    3. Executes the task function, when the Jupyter Notebook Server is terminated.

    Args:
        child_process (multiprocessing.Process, optional): The process to be terminated.
        post_execute (function, optional): The function to be executed before the jupyter notebook server is terminated.
    """
    child_process.join()

    if post_execute is not None:
        post_execute()
        logger.info("Post execute function executed successfully!")

    # Get the actual function from the task.
    while hasattr(task_function, "__wrapped__"):
        if isinstance(task_function, jupyter):
            task_function = task_function.__wrapped__
            break
        task_function = task_function.__wrapped__
    return task_function(*args, **kwargs)


JUPYTER_TYPE_VALUE = "jupyter"


def add_pod_name_and_namespace_to_pod_template(pod_template: PodTemplate) -> None:
    if pod_template.pod_spec is None:
        pod_template.pod_spec = client.V1PodSpec()

    if pod_template.pod_spec.containers is None or len(pod_template.pod_spec.containers) == 0:
        pod_template.pod_spec.containers = [client.V1Container(name="primary")]

    num_containers = len(pod_template.pod_spec.containers)

    primary_container: Optional[client.V1Container]
    if num_containers == 1:
        primary_container = pod_template.pod_spec.containers[0]
    else:
        primary_container = next(
            (container for container in pod_template.pod_spec.containers if container.name == "primary"),
            None,
        )

    if primary_container is None:
        raise ValueError(
            "More than one container is set in the pod template and unable to infer "
            "primary container. Set POD_NAME and POD_NAMESPACE environment variable on the primary "
            "container using kubernetes downward API."
        )

    if primary_container.env is None:
        primary_container.env = []

    if not any(env.name == "POD_NAME" for env in primary_container.env):
        primary_container.env += [
            client.V1EnvVar(
                name="POD_NAME",
                value_from=client.V1EnvVarSource(field_ref=client.V1ObjectFieldSelector(field_path="metadata.name")),
            ),
        ]

    if not any(env.name == "POD_NAMESPACE" for env in primary_container.env):
        primary_container.env += [
            client.V1EnvVar(
                name="POD_NAMESPACE",
                value_from=client.V1EnvVarSource(
                    field_ref=client.V1ObjectFieldSelector(field_path="metadata.namespace")
                ),
            ),
        ]


@dataclass
class JupyterConfig(object):
    enable: bool = True
    max_idle_seconds: int = MAX_IDLE_SECONDS
    port: int = 8080
    notebook_dir: str = "/root"
    path_override: Optional[str] = None
    pip_install_notebooks: bool = True


class JupyterFunctionTask(PythonFunctionTask[JupyterConfig]):
    """
    JupyterFunctionTask will to run a Jupyter Notebook server before the task starts:
    1. Launches and monitors the Jupyter Notebook server.
    2. Write Example Jupyter Notebook.
    3. Terminates if the server is idle for a set duration or user shuts down manually.

    The task is functionally similar to the flyteinteractive opensource package
    (plugins/flytekit-flyteinteractive) but has the key modifications:
        1. Is structured as a task type instead of a decorator. We do this to intercept
           the pod template the user passes in to inject environment variables we need.
        2. Installs jupyter to the system python so the base image does not need jupyter
           dependencies baked in.
        3. Starts a kubernetes headless service and creates an ambassador mapping so
           the notebook is accessible on `{flyte_url}/{pod_name}`.
    """

    def __init__(
        self,
        task_config: JupyterConfig,
        task_function: Callable,
        **kwargs,
    ):
        super().__init__(
            task_config=task_config,
            task_function=task_function,
            **kwargs,
        )
        # self.pod_template is set in the base class if pod_template is passed inside of kwargs.
        if self.pod_template is None:
            self.pod_template = PodTemplate()

        add_pod_name_and_namespace_to_pod_template(self.pod_template)

    def pre_execute(self, user_params: ExecutionParameters) -> ExecutionParameters:
        task_config = self.task_config
        ctx = FlyteContextManager.current_context()

        # 1. If disabled, we don't launch the Jupyter Notebook server.
        # 2. When user use pyflyte run or python to execute the task, we don't launch the Jupyter Notebook.
        #    Only when user use pyflyte run --remote to submit the task to cluster, we launch the Jupyter Notebook.
        if not task_config.enable or ctx.execution_state.is_local_execution():
            return user_params

        path = _must_get_jupyter_path(task_config.path_override, ctx.execution_state.is_local_execution())
        if task_config.pip_install_notebooks:
            _must_install_jupyter_dependencies()

        jupyterK8SResources = JupyterK8sResources(path, task_config.port)
        jupyterK8SResources.make_resources()

        # === Start of same logic as from flyteinteractive.

        # 1. Launches and monitors the Jupyter Notebook server.
        # The following line starts a Jupyter Notebook server with specific configurations:
        #   - '--port': Specifies the port number on which the server will listen for connections.
        #   - '--ip': *: Listen on all interfaces.
        #   - '--no-browser': No need for browser for remote connect
        #   - '--notebook-dir': Sets the directory where Jupyter Notebook will look for notebooks.
        #   - '--NotebookApp.token='': Disables token-based authentication by setting an empty token.
        #   - '--NotebookApp.base_url': Sets the base url of the jupyter notebook server.
        logger.info("Start the jupyter notebook server...")
        cmd = f"jupyter notebook --ip='*' --port {task_config.port} --no-browser --notebook-dir={task_config.notebook_dir} --NotebookApp.token='' --allow-root --NotebookApp.base_url {path}"

        #   - '--NotebookApp.shutdown_no_activity_timeout': Sets the maximum duration of inactivity
        #     before shutting down the Jupyter Notebook server automatically.
        # When shutdown_no_activity_timeout is 0, it means there is no idle timeout and it is always running.
        if task_config.max_idle_seconds:
            cmd += f" --NotebookApp.shutdown_no_activity_timeout={task_config.max_idle_seconds}"

        child_process = multiprocessing.Process(
            target=execute_command,
            kwargs={"cmd": cmd},
        )
        child_process.start()

        write_example_notebook(task_function=self.task_function, notebook_dir=task_config.notebook_dir)
        child_process.join()
        jupyterK8SResources.delete_resources()

        # === End of same logic as from flyteinteractive.

        return user_params


# Inject the Jupyter plugin into flytekits dynamic plugin loading system
TaskPlugins.register_pythontask_plugin(JupyterConfig, JupyterFunctionTask)
