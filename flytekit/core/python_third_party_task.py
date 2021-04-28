from __future__ import annotations

import importlib
import os
from typing import Any, Dict, List, Optional, TypeVar, Union

from flyteidl.core import tasks_pb2 as _tasks_pb2

from flytekit.common import utils as common_utils
from flytekit.common.tasks.raw_container import _get_container_definition
from flytekit.core.base_task import PythonTask, Task, TaskResolverMixin, ExecutableTaskMixin
from flytekit.core.context_manager import FlyteContext, Image, ImageConfig, SerializationSettings
from flytekit.core.resources import Resources, ResourceSpec
from flytekit.core.task_executor import FlyteTaskExecutor
from flytekit.core.tracker import TrackedInstance
from flytekit.loggers import logger
from flytekit.models import dynamic_job as _dynamic_job
from flytekit.models import literals as _literal_models
from flytekit.models import task as _task_model
from flytekit.models.core import identifier as identifier_models
from flytekit.models.security import Secret, SecurityContext

T = TypeVar("T")
TC = TypeVar("TC")


class PythonThirdPartyContainerTask(PythonTask[TC]):
    SERIALIZE_SETTINGS = SerializationSettings(
        project="PLACEHOLDER_PROJECT",
        domain="LOCAL",
        version="PLACEHOLDER_VERSION",
        env=None,
        image_config=ImageConfig(default_image=Image(name="thirdparty", fqn="flyteorg.io/placeholder", tag="image")),
    )

    def __init__(
        self,
        name: str,
        task_config: TC,
        container_image: str,
        executor: FlyteTaskExecutor,
        task_resolver: Optional[FlyteTaskResolver] = None,
        task_type="python-task",
        requests: Optional[Resources] = None,
        limits: Optional[Resources] = None,
        environment: Optional[Dict[str, str]] = None,
        secret_requests: Optional[List[Secret]] = None,
        **kwargs,
    ):
        """
        :param name: unique name for the task, usually the function's module and name.
        :param task_config: Configuration object for Task. Should be a unique type for that specific Task
        :param task_type: String task type to be associated with this Task
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
        self._resources = ResourceSpec(
            requests=requests if requests else Resources(), limits=limits if limits else Resources()
        )
        self._environment = environment
        self._executor = executor

        self._container_image = container_image
        # Because instances of these tasks rely on the task template in order to run even locally, we'll cache it
        self._task_template = None

        self._task_resolver = task_resolver or default_flyte_task_resolver

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        # Overriding base implementation to raise an error, force third-party task author to implement
        raise NotImplementedError

    def get_config(self, settings: SerializationSettings) -> Dict[str, str]:
        # Overriding base implementation but not doing anything. Technically this should be the task config,
        # but the IDL limitation that the value also has to be a string is very limiting.
        # Recommend putting information you need in the config into custom instead, because when serializing
        # the custom field, we jsonify custom and the place it into a protobuf struct. This config field
        # just gets put into a Dict[str, str]
        return {}

    @property
    def resources(self) -> ResourceSpec:
        return self._resources

    @property
    def executor(self) -> FlyteTaskExecutor:
        return self._executor

    @property
    def task_resolver(self) -> FlyteTaskResolver:
        return self._task_resolver

    @property
    def task_template(self) -> Optional[_task_model.TaskTemplate]:
        return self._task_template

    @property
    def container_image(self) -> str:
        return self._container_image

    def get_command(self, settings: SerializationSettings) -> List[str]:
        container_args = [
            "pyflyte-execute",
            "--inputs",
            "{{.input}}",
            "--output-prefix",
            "{{.outputPrefix}}",
            "--raw-output-data-prefix",
            "{{.rawOutputDataPrefix}}",
            "--resolver",
            self.task_resolver.location,
            "--",
            *self.task_resolver.loader_args(settings, self),
        ]

        return container_args

    def get_container(self, settings: SerializationSettings) -> _task_model.Container:
        env = {**settings.env, **self.environment} if self.environment else settings.env
        return _get_container_definition(
            image=self.container_image,
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

    def serialize_to_model(self, settings: SerializationSettings) -> _task_model.TaskTemplate:
        # This doesn't get called from translator unfortunately. Will need to move the translator to use the model
        # objects directly first.
        # Note: This doesn't settle the issue of duplicate registrations. We'll need to figure that out somehow.
        # TODO: After new control plane classes are in, promote the template to a FlyteTask, so that authors of
        #  third-party tasks have a familiar thing to work with.
        obj = _task_model.TaskTemplate(
            identifier_models.Identifier(
                identifier_models.ResourceType.TASK, settings.project, settings.domain, self.name, settings.version
            ),
            self.task_type,
            self.metadata.to_taskmetadata_model(),
            self.interface,
            self.get_custom(settings),
            container=self.get_container(settings),
            config=self.get_config(settings),
        )
        self._task_template = obj
        return obj

    def execute(self, **kwargs) -> Any:
        """
        This function overrides the default task execute behavior.

        Execution for third-party tasks is different from tasks that run the user workflow container.
        1. Serialize the task out to a TaskTemplate.
        2. Pass the template over to the Executor to run, along with the input arguments.
        3. Executor will reconstruct the Python task class object, before running the e

        When overridden for unit testing using the patch operator, all these steps will be skipped and the mocked code,
        which should just take in and return Python native values, will be run.
        """
        tt = self.task_template or self.serialize_to_model(settings=PythonThirdPartyContainerTask.SERIALIZE_SETTINGS)
        return self.executor.execute_from_model(tt, **kwargs)

    def dispatch_execute(
        self, ctx: FlyteContext, input_literal_map: _literal_models.LiteralMap
    ) -> Union[_literal_models.LiteralMap, _dynamic_job.DynamicJobSpec]:
        """
        This function overrides the default task execute behavior.
        """
        tt = self.task_template or self.serialize_to_model(settings=PythonThirdPartyContainerTask.SERIALIZE_SETTINGS)
        return self.executor.dispatch_execute(ctx, tt, input_literal_map)


def load_executor(object_location: str) -> Any:
    """
    Copied from entrypoint
    """
    class_obj = object_location.split(".")
    class_obj_mod = class_obj[:-1]  # e.g. ['flytekit', 'core', 'python_auto_container']
    class_obj_key = class_obj[-1]  # e.g. 'default_task_class_obj'
    class_obj_mod = importlib.import_module(".".join(class_obj_mod))
    return getattr(class_obj_mod, class_obj_key)


class FlyteTaskResolver(TrackedInstance, TaskResolverMixin):
    def __init__(self):
        super(FlyteTaskResolver, self).__init__()

    def name(self) -> str:
        return "task template resolver"

    def load_task(self, loader_args: List[str]) -> ExecutableTaskMixin:
        from flytekit.control_plane.tasks.task import FlyteTask

        logger.info(f"Task template loader args: {loader_args}")
        ctx = FlyteContext.current_context()
        task_template_local_path = os.path.join(ctx.execution_state.working_dir, "task_template.pb")
        ctx.file_access.get_data(loader_args[0], task_template_local_path)
        task_template_proto = common_utils.load_proto_from_file(_tasks_pb2.TaskTemplate, task_template_local_path)
        task_template_model = _task_model.TaskTemplate.from_flyte_idl(task_template_proto)

        executor = load_executor(loader_args[1])
        ft = FlyteTask.promote_from_model(task_template_model)
        ft._executor = executor
        return ft

    def loader_args(self, settings: SerializationSettings, t: PythonThirdPartyContainerTask) -> List[str]:
        return ["{{.taskTemplatePath}}", f"{t.executor.__module__}.{t.executor.__name__}"]

    def get_all_tasks(self) -> List[Task]:
        return []


default_flyte_task_resolver = FlyteTaskResolver()
