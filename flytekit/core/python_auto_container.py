from __future__ import annotations

import datetime as _datetime
import importlib
import logging
import os
import pathlib
import re
import traceback
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, TypeVar

from flyteidl.core import literals_pb2

from flytekit.common import constants
from flytekit.common import utils as common_utils
from flytekit.common.exceptions import scopes as _scoped_exceptions
from flytekit.common.tasks.raw_container import _get_container_definition
from flytekit.common.tasks.sdk_runnable import ExecutionParameters
from flytekit.configuration import internal as internal_config
from flytekit.configuration import platform as platform_config
from flytekit.configuration import sdk as sdk_config
from flytekit.core.base_task import IgnoreOutputs, PythonTask
from flytekit.core.context_manager import (
    ExecutionState,
    FlyteContext,
    ImageConfig,
    SerializationSettings,
    get_image_config,
)
from flytekit.core.promise import VoidPromise
from flytekit.core.resources import Resources, ResourceSpec
from flytekit.core.tracker import TrackedInstance
from flytekit.interfaces.data import data_proxy
from flytekit.interfaces.data.gcs import gcs_proxy
from flytekit.interfaces.data.s3 import s3proxy
from flytekit.interfaces.stats.taggable import get_stats
from flytekit.loggers import logger
from flytekit.models import dynamic_job
from flytekit.models import literals as literal_models
from flytekit.models import task as _task_model
from flytekit.models.core import errors as error_models
from flytekit.models.core import identifier as identifier_models
from flytekit.models.security import Secret, SecurityContext

T = TypeVar("T")


class FlyteTrackedABC(type(TrackedInstance), type(ABC)):
    """
    This class exists because if you try to inherit from abc.ABC and TrackedInstance by itself, you'll get the
    well-known ``TypeError: metaclass conflict: the metaclass of a derived class must be a (non-strict) subclass
    of the metaclasses of all its bases`` error.
    """


class PythonAutoContainerTask(PythonTask[T], metaclass=FlyteTrackedABC):
    """
    A Python AutoContainer task should be used as the base for all extensions that want the user's code to be in the
    container and the container information to be automatically captured.
    This base will auto configure the image and image version to be used for all its derivatives.

    If you are looking to extend, you might prefer to use ``PythonFunctionTask`` or ``PythonInstanceTask``
    """

    def __init__(
        self,
        name: str,
        task_config: T,
        task_type="python-task",
        container_image: Optional[str] = None,
        requests: Optional[Resources] = None,
        limits: Optional[Resources] = None,
        environment: Optional[Dict[str, str]] = None,
        task_resolver: Optional[TaskResolverMixin] = None,
        secret_requests: Optional[List[Secret]] = None,
        **kwargs,
    ):
        """
        :param name: unique name for the task, usually the function's module and name.
        :param task_config: Configuration object for Task. Should be a unique type for that specific Task
        :param task_type: String task type to be associated with this Task
        :param container_image: String FQN for the image.
        :param requests: custom resource request settings.
        :param limits: custom resource limit settings.
        :param environment: Environment variables you want the task to have when run.
        :param task_resolver: Custom resolver - will pick up the default resolver if empty, or the resolver set
          in the compilation context if one is set.
        :param List[Secret] secret_requests: Secrets that are requested by this container execution. These secrets will
                                           be mounted based on the configuration in the Secret and available through
                                           the SecretManager using the name of the secret as the group
                                           Ideally the secret keys should also be semi-descriptive.
                                           The key values will be available from runtime, if the backend is configured
                       to provide secrets and if secrets are available in the configured secrets store.
                       Possible options for secret stores are
                        - `Vault <https://www.vaultproject.io/>`,
                        - `Confidant <https://lyft.github.io/confidant/>`,
                        - `Kube secrets <https://kubernetes.io/docs/concepts/configuration/secret/>`
                        - `AWS Parameter store <https://docs.aws.amazon.com/systems-manager/latest/userguide/systems-manager-parameter-store.html>`_
                        etc
        """
        sec_ctx = None
        if secret_requests:
            for s in secret_requests:
                if not isinstance(s, Secret):
                    raise AssertionError(f"Secret {s} should be of type flytekit.Secret, received {type(s)}")
            sec_ctx = SecurityContext(secrets=secret_requests)
        super().__init__(
            task_type=task_type,
            name=name,
            task_config=task_config,
            security_ctx=sec_ctx,
            **kwargs,
        )
        self._container_image = container_image
        # TODO(katrogan): Implement resource overrides
        self._resources = ResourceSpec(
            requests=requests if requests else Resources(), limits=limits if limits else Resources()
        )
        self._environment = environment

        compilation_state = FlyteContext.current_context().compilation_state
        if compilation_state and compilation_state.task_resolver:
            if task_resolver:
                logger.info(
                    f"Not using the passed in task resolver {task_resolver} because one found in compilation context"
                )
            self._task_resolver = compilation_state.task_resolver
            if self._task_resolver.task_name(self) is not None:
                self._name = self._task_resolver.task_name(self)
        else:
            self._task_resolver = task_resolver or default_task_resolver

    @property
    def task_resolver(self) -> TaskResolverMixin:
        return self._task_resolver

    @property
    def container_image(self) -> Optional[str]:
        return self._container_image

    @property
    def resources(self) -> ResourceSpec:
        return self._resources

    @abstractmethod
    def get_command(self, settings: SerializationSettings) -> List[str]:
        pass

    def get_target(self, settings: SerializationSettings) -> _task_model.Container:
        ...

    def get_container(self, settings: SerializationSettings) -> _task_model.Container:
        env = {**settings.env, **self.environment} if self.environment else settings.env
        return _get_container_definition(
            image=get_registerable_container_image(self.container_image, settings.image_config),
            command=[],
            args=self.get_command(settings=settings),
            data_loading_config=None,
            environment=env,
            storage_request=self.resources.requests.storage,
            cpu_request=self.resources.requests.cpu,
            gpu_request=self.resources.requests.gpu,
            memory_request=self.resources.requests.mem,
            storage_limit=self.resources.limits.storage,
            cpu_limit=self.resources.limits.cpu,
            gpu_limit=self.resources.limits.gpu,
            memory_limit=self.resources.limits.mem,
        )


class TaskResolverMixin(object):
    """
    Flytekit tasks interact with the Flyte platform very, very broadly in two steps. They need to be uploaded to Admin,
    and then they are run by the user upon request (either as a single task execution or as part of a workflow). In any
    case, at execution time, the container image containing the task needs to be spun up again (for container tasks at
    least which most tasks are) at which point the container needs to know which task it's supposed to run and
    how to rehydrate the task object.

    For example, the serialization of a simple task ::

        # in repo_root/workflows/example.py
        @task
        def t1(...) -> ...: ...

    might result in a container with arguments like ::

        pyflyte-execute --inputs s3://path/inputs.pb --output-prefix s3://outputs/location \
        --raw-output-data-prefix /tmp/data \
        --resolver flytekit.core.python_auto_container.default_task_resolver \
        -- \
        task-module repo_root.workflows.example task-name t1

    At serialization time, the container created for the task will start out automatically with the ``pyflyte-execute``
    bit, along with the requisite input/output args and the offloaded data prefix. Appended to that will be two things,

    #. the ``location`` of the task's task resolver, followed by two dashes, followed by
    #. the arguments provided by calling the ``loader_args`` function below.

    The ``default_task_resolver`` declared below knows that ::

    * When ``loader_args`` is called on a task, to look up the module the task is in, and the name of the task (the
      key of the task in the module, either the function name, or the variable it was assigned to).
    * When ``load_task`` is called, it interprets the first part of the command as the module to call
    ``importlib.import_module`` on, and then looks for a key ``t1``.

    This is just the default behavior. Users should feel free to implement their own resolvers.
    """

    @property
    @abstractmethod
    def location(self) -> str:
        pass

    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def load_task(self, loader_args: List[str]) -> PythonAutoContainerTask:
        """
        Given the set of identifier keys, should return one Python Task or raise an error if not found
        """
        pass

    @abstractmethod
    def loader_args(self, settings: SerializationSettings, t: PythonAutoContainerTask) -> List[str]:
        """
        Return a list of strings that can help identify the parameter Task
        """
        pass

    @abstractmethod
    def get_all_tasks(self) -> List[PythonAutoContainerTask]:
        """
        Future proof method. Just making it easy to access all tasks (Not required today as we auto register them)
        """
        pass

    def task_name(self, t: PythonAutoContainerTask) -> Optional[str]:
        """
        Overridable function that can optionally return a custom name for a given task
        """
        return None


class DefaultTaskResolver(TrackedInstance, TaskResolverMixin):
    """
    Please see the notes in the TaskResolverMixin as it describes this default behavior.
    """

    def name(self) -> str:
        return "DefaultTaskResolver"

    def load_task(self, loader_args: List[str]) -> PythonAutoContainerTask:
        _, task_module, _, task_name, *_ = loader_args

        task_module = importlib.import_module(task_module)
        task_def = getattr(task_module, task_name)
        return task_def

    def loader_args(self, settings: SerializationSettings, task: PythonAutoContainerTask) -> List[str]:
        from flytekit.core.python_function_task import PythonFunctionTask

        if isinstance(task, PythonFunctionTask):
            return [
                "task-module",
                task.task_function.__module__,
                "task-name",
                task.task_function.__name__,
            ]
        if isinstance(task, TrackedInstance):
            return [
                "task-module",
                task.instantiated_in,
                "task-name",
                task.lhs,
            ]

    def get_all_tasks(self) -> List[PythonAutoContainerTask]:
        raise Exception("should not be needed")


default_task_resolver = DefaultTaskResolver()


def get_registerable_container_image(img: Optional[str], cfg: ImageConfig) -> str:
    """
    :param img: Configured image
    :param cfg: Registration configuration
    :return:
    """
    if img is not None and img != "":
        matches = _IMAGE_REPLACE_REGEX.findall(img)
        if matches is None or len(matches) == 0:
            return img
        for m in matches:
            if len(m) < 3:
                raise AssertionError(
                    "Image specification should be of the form <fqn>:<tag> OR <fqn>:{{.image.default.version}} OR "
                    f"{{.image.xyz.fqn}}:{{.image.xyz.version}} OR {{.image.xyz}} - Received {m}"
                )
            replace_group, name, attr = m
            if name is None or name == "":
                raise AssertionError(f"Image format is incorrect {m}")
            img_cfg = cfg.find_image(name)
            if img_cfg is None:
                raise AssertionError(f"Image Config with name {name} not found in the configuration")
            if attr == "version":
                if img_cfg.tag is not None:
                    img = img.replace(replace_group, img_cfg.tag)
                else:
                    img = img.replace(replace_group, cfg.default_image.tag)
            elif attr == "fqn":
                img = img.replace(replace_group, img_cfg.fqn)
            elif attr == "":
                img = img.replace(replace_group, img_cfg.full)
            else:
                raise AssertionError(f"Only fqn and version are supported replacements, {attr} is not supported")
        return img
    return f"{cfg.default_image.fqn}:{cfg.default_image.tag}"


# Matches {{.image.<name>.<attr>}}. A name can be either 'default' indicating the default image passed during
# serialization or it can be a custom name for an image that must be defined in the config section Images. An attribute
# can be either 'fqn', 'version' or non-existent.
# fqn will access the fully qualified name of the image (e.g. registry/imagename:version -> registry/imagename)
# version will access the version part of the image (e.g. registry/imagename:version -> version)
# With empty attribute, it'll access the full image path (e.g. registry/imagename:version -> registry/imagename:version)
_IMAGE_REPLACE_REGEX = re.compile(r"({{\s*\.image[s]?(?:\.([a-zA-Z]+))(?:\.([a-zA-Z]+))?\s*}})", re.IGNORECASE)


class ExecutionContainer(object):
    """
    ExecutionContainers are the bridge between the authoring experience of flytekit and the execution side of flytekit.
    They tie together the serialization part of a task at compile (a.k.a. serialization time) with what happens
    when the task is run on a production cluster
    """

    def __init__(self, default_image: Optional[str] = None, environment: Optional[Dict[str, str]] = None):
        self._environment = environment
        self._default_image = default_image

    @property
    def default_image(self) -> str:
        return self._default_image or ""

    @property
    def environment(self) -> Optional[Dict[str, str]]:
        return self._environment

    def get_command(self, settings: SerializationSettings):
        raise NotImplementedError("must override get_command")

    def get_container(self, settings: SerializationSettings, task: PythonAutoContainerTask) -> _task_model.Container:
        env = {**settings.env, **self._environment} if self._environment else settings.env
        return _get_container_definition(
            image=self.default_image,
            command=[],
            args=self.get_command(settings=settings),
            data_loading_config=None,
            environment=env,
        )

    def run(self, inputs, output_prefix, raw_output_data_prefix, task_template_path):
        raise NotImplementedError("must override run")

    def handle_annotated_task(
        self, task_def: PythonAutoContainerTask, inputs: str, output_prefix: str, raw_output_data_prefix: str
    ):
        """
        Entrypoint for all PythonAutoContainerTask extensions
        """
        cloud_provider = platform_config.CLOUD_PROVIDER.get()
        log_level = internal_config.LOGGING_LEVEL.get() or sdk_config.LOGGING_LEVEL.get()
        logging.getLogger().setLevel(log_level)

        ctx = FlyteContext.current_context()

        # Create directories
        user_workspace_dir = ctx.file_access.local_access.get_random_directory()
        pathlib.Path(user_workspace_dir).mkdir(parents=True, exist_ok=True)
        from flytekit import __version__ as _api_version

        execution_parameters = ExecutionParameters(
            execution_id=identifier_models.WorkflowExecutionIdentifier(
                project=internal_config.EXECUTION_PROJECT.get(),
                domain=internal_config.EXECUTION_DOMAIN.get(),
                name=internal_config.EXECUTION_NAME.get(),
            ),
            execution_date=_datetime.datetime.utcnow(),
            stats=get_stats(
                # Stats metric path will be:
                # registration_project.registration_domain.app.module.task_name.user_stats
                # and it will be tagged with execution-level values for project/domain/wf/lp
                "{}.{}.{}.user_stats".format(
                    internal_config.TASK_PROJECT.get() or internal_config.PROJECT.get(),
                    internal_config.TASK_DOMAIN.get() or internal_config.DOMAIN.get(),
                    internal_config.TASK_NAME.get() or internal_config.NAME.get(),
                ),
                tags={
                    "exec_project": internal_config.EXECUTION_PROJECT.get(),
                    "exec_domain": internal_config.EXECUTION_DOMAIN.get(),
                    "exec_workflow": internal_config.EXECUTION_WORKFLOW.get(),
                    "exec_launchplan": internal_config.EXECUTION_LAUNCHPLAN.get(),
                    "api_version": _api_version,
                },
            ),
            logging=logging,
            tmp_dir=user_workspace_dir,
        )

        if cloud_provider == constants.CloudProvider.AWS:
            file_access = data_proxy.FileAccessProvider(
                local_sandbox_dir=sdk_config.LOCAL_SANDBOX.get(),
                remote_proxy=s3proxy.AwsS3Proxy(raw_output_data_prefix),
            )
        elif cloud_provider == constants.CloudProvider.GCP:
            file_access = data_proxy.FileAccessProvider(
                local_sandbox_dir=sdk_config.LOCAL_SANDBOX.get(),
                remote_proxy=gcs_proxy.GCSProxy(raw_output_data_prefix),
            )
        elif cloud_provider == constants.CloudProvider.LOCAL:
            # A fake remote using the local disk will automatically be created
            file_access = data_proxy.FileAccessProvider(local_sandbox_dir=_sdk_config.LOCAL_SANDBOX.get())
        else:
            raise Exception(f"Bad cloud provider {cloud_provider}")

        with ctx.new_file_access_context(file_access_provider=file_access) as ctx:
            # TODO: This is copied from serialize, which means there's a similarity here I'm not seeing.
            env = {
                internal_config.CONFIGURATION_PATH.env_var: internal_config.CONFIGURATION_PATH.get(),
                internal_config.IMAGE.env_var: internal_config.IMAGE.get(),
            }

            serialization_settings = SerializationSettings(
                project=internal_config.TASK_PROJECT.get(),
                domain=internal_config.TASK_DOMAIN.get(),
                version=internal_config.TASK_VERSION.get(),
                image_config=get_image_config(),
                env=env,
            )

            # The reason we need this is because of dynamic tasks. Even if we move compilation all to Admin,
            # if a dynamic task calls some task, t1, we have to write to the DJ Spec the correct task
            # identifier for t1.
            with ctx.new_serialization_settings(serialization_settings=serialization_settings) as ctx:
                # Because execution states do not look up the context chain, it has to be made last
                with ctx.new_execution_context(
                    mode=ExecutionState.Mode.TASK_EXECUTION, execution_params=execution_parameters
                ) as ctx:
                    self.dispatch_execute(ctx, task_def, inputs, output_prefix)

    def dispatch_execute(
        self, ctx: FlyteContext, task_def: PythonAutoContainerTask, inputs_path: str, output_prefix: str
    ):
        """
        Dispatches execute to PythonTask
            Step1: Download inputs and load into a literal map
            Step2: Invoke task - dispatch_execute
            Step3:
                a: [Optional] Record outputs to output_prefix
                b: OR if IgnoreOutputs is raised, then ignore uploading outputs
                c: OR if an unhandled exception is retrieved - record it as an errors.pb
        """
        output_file_dict = {}
        try:
            # Step1
            local_inputs_file = os.path.join(ctx.execution_state.working_dir, "inputs.pb")
            ctx.file_access.get_data(inputs_path, local_inputs_file)
            input_proto = common_utils.load_proto_from_file(literals_pb2.LiteralMap, local_inputs_file)
            idl_input_literals = literal_models.LiteralMap.from_flyte_idl(input_proto)
            # Step2
            outputs = task_def.dispatch_execute(ctx, idl_input_literals)
            # Step3a
            if isinstance(outputs, VoidPromise):
                logging.getLogger().warning("Task produces no outputs")
                output_file_dict = {constants.OUTPUT_FILE_NAME: literal_models.LiteralMap(literals={})}
            elif isinstance(outputs, literal_models.LiteralMap):
                output_file_dict = {constants.OUTPUT_FILE_NAME: outputs}
            elif isinstance(outputs, dynamic_job.DynamicJobSpec):
                output_file_dict = {constants.FUTURES_FILE_NAME: outputs}
            else:
                logging.getLogger().error(f"SystemError: received unknown outputs from task {outputs}")
                output_file_dict[constants.ERROR_FILE_NAME] = error_models.ErrorDocument(
                    error_models.ContainerError(
                        "UNKNOWN_OUTPUT",
                        f"Type of output received not handled {type(outputs)} outputs: {outputs}",
                        error_models.ContainerError.Kind.RECOVERABLE,
                    )
                )
        except _scoped_exceptions.FlyteScopedException as e:
            logging.error("!! Begin Error Captured by Flyte !!")
            output_file_dict[constants.ERROR_FILE_NAME] = error_models.ErrorDocument(
                error_models.ContainerError(e.error_code, e.verbose_message, e.kind)
            )
            logging.error(e.verbose_message)
            logging.error("!! End Error Captured by Flyte !!")
        except Exception as e:
            if isinstance(e, IgnoreOutputs):
                # Step 3b
                logging.warning(f"IgnoreOutputs received! Outputs.pb will not be uploaded. reason {e}")
                return
            # Step 3c
            logging.error(f"Exception when executing task {task_def.name}, reason {str(e)}")
            logging.error("!! Begin Unknown System Error Captured by Flyte !!")
            exc_str = traceback.format_exc()
            output_file_dict[constants.ERROR_FILE_NAME] = error_models.ErrorDocument(
                error_models.ContainerError(
                    "SYSTEM:Unknown",
                    exc_str,
                    error_models.ContainerError.Kind.RECOVERABLE,
                )
            )
            logging.error(exc_str)
            logging.error("!! End Error Captured by Flyte !!")

        for k, v in output_file_dict.items():
            common_utils.write_proto_to_file(v.to_flyte_idl(), os.path.join(ctx.execution_state.engine_dir, k))

        ctx.file_access.upload_directory(ctx.execution_state.engine_dir, output_prefix)
        logging.info(f"Engine folder written successfully to the output prefix {output_prefix}")
