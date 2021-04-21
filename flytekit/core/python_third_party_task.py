from __future__ import annotations

from abc import abstractmethod
from typing import Any, Dict, Generic, List, Optional, TypeVar

from flytekit.common.tasks.raw_container import _get_container_definition
from flytekit.core.base_task import PythonTask
from flytekit.core.context_manager import Image, ImageConfig, SerializationSettings
from flytekit.core.resources import Resources, ResourceSpec
from flytekit.core.tracker import TrackedInstance
from flytekit.models import task as _task_model
from flytekit.models.security import Secret, SecurityContext

T = TypeVar("T")
TC = TypeVar("TC")


class TaskTemplateExecutor(TrackedInstance, Generic[T]):
    @classmethod
    def execute_from_template(cls, tt: _task_model.TaskTemplate, **kwargs) -> Any:
        task = cls.promote_from_template(tt)
        return cls.native_execute(task, **kwargs)

    @classmethod
    def native_execute(cls, task: T, **kwargs) -> Any:
        raise NotImplementedError

    @classmethod
    def promote_from_template(cls, tt: _task_model.TaskTemplate) -> T:
        raise NotImplementedError


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
        executor: TaskTemplateExecutor,
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

    @property
    def resources(self) -> ResourceSpec:
        return self._resources

    @abstractmethod
    def get_command(self, settings: SerializationSettings) -> List[str]:
        pass

    @property
    def executor(self) -> TaskTemplateExecutor:
        return self._executor

    @property
    def task_template(self) -> Optional[_task_model.TaskTemplate]:
        return self._task_template

    @property
    def container_image(self) -> str:
        return self._container_image

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

    def serialize_to_template(self, settings: SerializationSettings) -> _task_model.TaskTemplate:
        raise NotImplementedError

    def execute(self, **kwargs) -> Any:
        """
        Execution for third-party tasks is different from tasks that run the user workflow container.
        1. Serialize the task out to a TaskTemplate.
        2. Pass the template over to the Executor to run, along with the input arguments.
        3. Executor will reconstruct the Python task class object, before running the e

        When overridden for unit testing using the patch operator, all these steps will be skipped and the mocked code,
        which should just take in and return Python native values, will be run.
        """
        tt = self.task_template or self.serialize_to_template(settings=PythonThirdPartyContainerTask.SERIALIZE_SETTINGS)
        return self.executor.execute_from_template(tt, **kwargs)

