from __future__ import annotations

from abc import abstractmethod
from typing import Any, Dict, Generic, List, Optional, TypeVar, Union
from flytekit.core.context_manager import (
    BranchEvalMode,
    ExecutionState,
    FlyteContext,
    FlyteEntities,
    SerializationSettings,
)
from flytekit.core.type_engine import TypeEngine
from flytekit.loggers import logger
from flytekit.common.tasks.sdk_runnable import ExecutionParameters
from flytekit.common.tasks.raw_container import _get_container_definition
from flytekit.core.base_task import PythonTask
from flytekit.core.context_manager import Image, ImageConfig, SerializationSettings
from flytekit.core.resources import Resources, ResourceSpec
from flytekit.core.tracker import TrackedInstance
from flytekit.models import task as _task_model, literals as _literal_models
from flytekit.models import dynamic_job as _dynamic_job
from flytekit.models.core import identifier as identifier_models
from flytekit.models.security import Secret, SecurityContext

T = TypeVar("T")
TC = TypeVar("TC")


class TaskTemplateExecutor(TrackedInstance, Generic[T]):
    @classmethod
    def execute_from_model(cls, tt: _task_model.TaskTemplate, **kwargs) -> Any:
        raise NotImplementedError

    @classmethod
    def pre_execute(cls, user_params: ExecutionParameters) -> ExecutionParameters:
        """
        This function is a stub, just here to keep dispatch_execute compatibility between this class and PythonTask.
        """
        return user_params

    @classmethod
    def post_execute(cls, user_params: ExecutionParameters, rval: Any) -> Any:
        """
        This function is a stub, just here to keep dispatch_execute compatibility between this class and PythonTask.
        """
        return rval

    @classmethod
    def dispatch_execute(
        cls, ctx: FlyteContext, tt: _task_model.TaskTemplate, input_literal_map: _literal_models.LiteralMap
    ) -> Union[_literal_models.LiteralMap, _dynamic_job.DynamicJobSpec]:
        """
        This function is copied from PythonTask.dispatch_execute. Will need to make it a mixin and refactor in the
        future.
        """

        # Invoked before the task is executed
        new_user_params = cls.pre_execute(ctx.user_space_params)

        # Create another execution context with the new user params, but let's keep the same working dir
        with ctx.new_execution_context(
            mode=ctx.execution_state.mode,
            execution_params=new_user_params,
            working_dir=ctx.execution_state.working_dir,
        ) as exec_ctx:
            # Added: Have to reverse the Python interface from the task template Flyte interface
            #  This will be moved into the FlyteTask promote logic instead
            guessed_python_input_types = TypeEngine.guess_python_types(tt.interface.inputs)
            native_inputs = TypeEngine.literal_map_to_kwargs(exec_ctx, input_literal_map, guessed_python_input_types)

            logger.info(f"Invoking FlyteTask executor {tt.id.name} with inputs: {native_inputs}")
            try:
                native_outputs = cls.execute_from_model(tt, **native_inputs)
            except Exception as e:
                logger.exception(f"Exception when executing {e}")
                raise e

            logger.info(f"Task executed successfully in user level, outputs: {native_outputs}")
            # Lets run the post_execute method. This may result in a IgnoreOutputs Exception, which is
            # bubbled up to be handled at the callee layer.
            native_outputs = cls.post_execute(new_user_params, native_outputs)

            # Short circuit the translation to literal map because what's returned may be a dj spec (or an
            # already-constructed LiteralMap if the dynamic task was a no-op), not python native values
            if isinstance(native_outputs, _literal_models.LiteralMap) or isinstance(
                native_outputs, _dynamic_job.DynamicJobSpec
            ):
                return native_outputs

            expected_output_names = list(tt.interface.outputs.keys())
            if len(expected_output_names) == 1:
                # Here we have to handle the fact that the task could've been declared with a typing.NamedTuple of
                # length one. That convention is used for naming outputs - and single-length-NamedTuples are
                # particularly troublesome but elegant handling of them is not a high priority
                # Again, we're using the output_tuple_name as a proxy.
                # Deleted some stuff
                native_outputs_as_map = {expected_output_names[0]: native_outputs}
            elif len(expected_output_names) == 0:
                native_outputs_as_map = {}
            else:
                native_outputs_as_map = {
                    expected_output_names[i]: native_outputs[i] for i, _ in enumerate(native_outputs)
                }

            # We manually construct a LiteralMap here because task inputs and outputs actually violate the assumption
            # built into the IDL that all the values of a literal map are of the same type.
            literals = {}
            for k, v in native_outputs_as_map.items():
                literal_type = tt.interface.outputs[k].type
                py_type = type(v)

                if isinstance(v, tuple):
                    raise AssertionError(f"Output({k}) in task{tt.id.name} received a tuple {v}, instead of {py_type}")
                try:
                    literals[k] = TypeEngine.to_literal(exec_ctx, v, py_type, literal_type)
                except Exception as e:
                    raise AssertionError(f"failed to convert return value for var {k}") from e

            outputs_literal_map = _literal_models.LiteralMap(literals=literals)
            # After the execute has been successfully completed
            return outputs_literal_map


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
