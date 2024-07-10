from __future__ import annotations

import datetime
from functools import update_wrapper
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Type, TypeVar, Union, overload
import tempfile
import os
import pathlib
from typing import Dict
import gzip

from flytekit.configuration import Config, ImageConfig, SerializationSettings, FastSerializationSettings
from flytekit.core import launch_plan as _annotated_launchplan
from flytekit.core import workflow as _annotated_workflow
from flytekit.core.base_task import TaskResolverMixin
from flytekit.core.pod_template import PodTemplate
from flytekit.core.python_function_task import PythonFunctionTask
from flytekit.core.resources import Resources
from flytekit.core.task import task
from flytekit.extras.accelerators import BaseAccelerator
from flytekit.image_spec.image_spec import ImageSpec
from flytekit.models.documentation import Documentation
from flytekit.models.security import Secret
from flytekit.remote import FlyteRemote
from flytekit.remote.entities import FlyteTask
from flytekit.tools.translator import Options

T = TypeVar("T")
FuncOut = TypeVar("FuncOut")

class RemotePythonTask(PythonFunctionTask[T]):  # type: ignore
    """
    A RemotePythonTask is a PythonFunctionTask that is meant to be executed remotely. It is a subclass of
    PythonFunctionTask and has all the same properties, but it is meant to be used in a different context.
    """

    def __init__(
        self,
        remote_entry: FlyteRemote,
        task: PythonFunctionTask[T],
    ):
        # inherit all the properties of the task
        self.__dict__.update(task.__dict__)
        self.remote_entry = remote_entry

    def remote(
        self,
        inputs: Dict[str, Any],
        project: str = None,
        domain: str = None,
        name: str = None,
        version: str = None,
        execution_name: Optional[str] = None,
        execution_name_prefix: Optional[str] = None,
        image_config: Optional[ImageConfig] = None,
        options: Optional[Options] = None,
        wait: bool = False,
        type_hints: Optional[Dict[str, Type]] = None,
        overwrite_cache: Optional[bool] = None,
        envs: Optional[Dict[str, str]] = None,
        tags: Optional[List[str]] = None,
        cluster_pool: Optional[str] = None,
        execution_cluster_label: Optional[str] = None,
    ) -> Any:
        # return self.remote_entry.execute(
        #     self, 
        #     inputs=inputs,
        #     project=project,
        #     domain=domain,
        #     name=name,
        #     version=version,
        #     execution_name=execution_name,
        #     execution_name_prefix=execution_name_prefix,
        #     image_config=image_config,
        #     options=options,
        #     wait=wait,
        #     type_hints=type_hints,
        #     overwrite_cache=overwrite_cache,
        #     envs=envs,
        #     tags=tags,
        #     cluster_pool=cluster_pool,
        #     execution_cluster_label=execution_cluster_label,
        # )
        # if interactive: # Ignore reference tasks
        import cloudpickle
        with tempfile.TemporaryDirectory() as tmp_dir:
            dest = pathlib.Path(os.path.join(tmp_dir, "pkl.gz"))
            with gzip.GzipFile(filename=dest, mode="wb", mtime=0) as gzipped:
                cloudpickle.dump(self, gzipped)
            md5_bytes, upload_native_url = self.remote_entry.upload_file(
                dest, project or self.remote_entry.default_project, domain or self.remote_entry.default_domain
            )
            ss = SerializationSettings(
                image_config=image_config or ImageConfig.auto_default_image(),
                project=project or self.remote_entry.default_project,
                domain=domain or self.remote_entry._default_domain,
                version=version,
                fast_serialization_settings=FastSerializationSettings(
                    enabled=True, 
                    pickled=True, 
                    distribution_location=upload_native_url),
            )
            if not version:
                version = self.remote_entry._version_from_hash(md5_bytes, ss)
                ss.version = version
            flyte_task: FlyteTask = self.remote_entry.register_task(self, ss)
        # else:
        return self.remote_entry.execute(
            flyte_task, 
            inputs=inputs,
            project=project,
            domain=domain,
            name=name,
            version=version,
            execution_name=execution_name,
            execution_name_prefix=execution_name_prefix,
            image_config=image_config,
            options=options,
            wait=wait,
            type_hints=type_hints,
            overwrite_cache=overwrite_cache,
            envs=envs,
            tags=tags,
            cluster_pool=cluster_pool,
            execution_cluster_label=execution_cluster_label,
        )

class RemoteTask:
    @overload
    def task_decorator(
        self,
        _task_function: None = ...,
        task_config: Optional[T] = ...,
        cache: bool = ...,
        cache_serialize: bool = ...,
        cache_version: str = ...,
        cache_ignore_input_vars: Tuple[str, ...] = ...,
        retries: int = ...,
        interruptible: Optional[bool] = ...,
        deprecated: str = ...,
        timeout: Union[datetime.timedelta, int] = ...,
        container_image: Optional[Union[str, ImageSpec]] = ...,
        environment: Optional[Dict[str, str]] = ...,
        requests: Optional[Resources] = ...,
        limits: Optional[Resources] = ...,
        secret_requests: Optional[List[Secret]] = ...,
        execution_mode: PythonFunctionTask.ExecutionBehavior = ...,
        node_dependency_hints: Optional[
            Iterable[
                Union[
                    PythonFunctionTask,
                    _annotated_launchplan.LaunchPlan,
                    _annotated_workflow.WorkflowBase,
                ]
            ]
        ] = ...,
        task_resolver: Optional[TaskResolverMixin] = ...,
        docs: Optional[Documentation] = ...,
        disable_deck: Optional[bool] = ...,
        enable_deck: Optional[bool] = ...,
        pod_template: Optional["PodTemplate"] = ...,
        pod_template_name: Optional[str] = ...,
        accelerator: Optional[BaseAccelerator] = ...,
    ) -> Callable[[Callable[..., FuncOut]], PythonFunctionTask[T]]: ...


    @overload
    def task_decorator(
        self,
        _task_function: Callable[..., FuncOut],
        task_config: Optional[T] = ...,
        cache: bool = ...,
        cache_serialize: bool = ...,
        cache_version: str = ...,
        cache_ignore_input_vars: Tuple[str, ...] = ...,
        retries: int = ...,
        interruptible: Optional[bool] = ...,
        deprecated: str = ...,
        timeout: Union[datetime.timedelta, int] = ...,
        container_image: Optional[Union[str, ImageSpec]] = ...,
        environment: Optional[Dict[str, str]] = ...,
        requests: Optional[Resources] = ...,
        limits: Optional[Resources] = ...,
        secret_requests: Optional[List[Secret]] = ...,
        execution_mode: PythonFunctionTask.ExecutionBehavior = ...,
        node_dependency_hints: Optional[
            Iterable[
                Union[
                    PythonFunctionTask,
                    _annotated_launchplan.LaunchPlan,
                    _annotated_workflow.WorkflowBase,
                ]
            ]
        ] = ...,
        task_resolver: Optional[TaskResolverMixin] = ...,
        docs: Optional[Documentation] = ...,
        disable_deck: Optional[bool] = ...,
        enable_deck: Optional[bool] = ...,
        pod_template: Optional["PodTemplate"] = ...,
        pod_template_name: Optional[str] = ...,
        accelerator: Optional[BaseAccelerator] = ...,
    ) -> Union[PythonFunctionTask[T], Callable[..., FuncOut]]: ...


    def task_decorator(
        self,
        _task_function: Optional[Callable[..., FuncOut]] = None,
        task_config: Optional[T] = None,
        cache: bool = False,
        cache_serialize: bool = False,
        cache_version: str = "",
        cache_ignore_input_vars: Tuple[str, ...] = (),
        retries: int = 0,
        interruptible: Optional[bool] = None,
        deprecated: str = "",
        timeout: Union[datetime.timedelta, int] = 0,
        container_image: Optional[Union[str, ImageSpec]] = None,
        environment: Optional[Dict[str, str]] = None,
        requests: Optional[Resources] = None,
        limits: Optional[Resources] = None,
        secret_requests: Optional[List[Secret]] = None,
        execution_mode: PythonFunctionTask.ExecutionBehavior = PythonFunctionTask.ExecutionBehavior.DEFAULT,
        node_dependency_hints: Optional[
            Iterable[
                Union[
                    PythonFunctionTask,
                    _annotated_launchplan.LaunchPlan,
                    _annotated_workflow.WorkflowBase,
                ]
            ]
        ] = None,
        task_resolver: Optional[TaskResolverMixin] = None,
        docs: Optional[Documentation] = None,
        disable_deck: Optional[bool] = None,
        enable_deck: Optional[bool] = None,
        pod_template: Optional["PodTemplate"] = None,
        pod_template_name: Optional[str] = None,
        accelerator: Optional[BaseAccelerator] = None,
    ) -> Union[
        Callable[[Callable[..., FuncOut]], PythonFunctionTask[T]],
        PythonFunctionTask[T],
        Callable[..., FuncOut],
    ]:
        """
        This is the core decorator to use for any task type in flytekit.

        Tasks are the building blocks of Flyte. They represent users code. Tasks have the following properties

        * Versioned (usually tied to the git revision SHA1)
        * Strong interfaces (specified inputs and outputs)
        * Declarative
        * Independently executable
        * Unit testable

        For a simple python task,

        .. code-block:: python

            @task
            def my_task(x: int, y: typing.Dict[str, str]) -> str:
                ...

        For specific task types

        .. code-block:: python

            @task(task_config=Spark(), retries=3)
            def my_task(x: int, y: typing.Dict[str, str]) -> str:
                ...

        Please see some cookbook :std:ref:`task examples <cookbook:tasks>` for additional information.

        :param _task_function: This argument is implicitly passed and represents the decorated function
        :param task_config: This argument provides configuration for a specific task types.
                            Please refer to the plugins documentation for the right object to use.
        :param cache: Boolean that indicates if caching should be enabled
        :param cache_serialize: Boolean that indicates if identical (ie. same inputs) instances of this task should be
            executed in serial when caching is enabled. This means that given multiple concurrent executions over
            identical inputs, only a single instance executes and the rest wait to reuse the cached results. This
            parameter does nothing without also setting the cache parameter.
        :param cache_version: Cache version to use. Changes to the task signature will automatically trigger a cache miss,
            but you can always manually update this field as well to force a cache miss. You should also manually bump
            this version if the function body/business logic has changed, but the signature hasn't.
        :param cache_ignore_input_vars: Input variables that should not be included when calculating hash for cache.
        :param retries: Number of times to retry this task during a workflow execution.
        :param interruptible: [Optional] Boolean that indicates that this task can be interrupted and/or scheduled on nodes
                            with lower QoS guarantees. This will directly reduce the `$`/`execution cost` associated,
                            at the cost of performance penalties due to potential interruptions. Requires additional
                            Flyte platform level configuration. If no value is provided, the task will inherit this
                            attribute from its workflow, as follows:
                            No values set for interruptible at the task or workflow level - task is not interruptible
                            Task has interruptible=True, but workflow has no value set - task is interruptible
                            Workflow has interruptible=True, but task has no value set - task is interruptible
                            Workflow has interruptible=False, but task has interruptible=True - task is interruptible
                            Workflow has interruptible=True, but task has interruptible=False - task is not interruptible
        :param deprecated: A string that can be used to provide a warning message for deprecated task. Absence / empty str
                        indicates that the task is active and not deprecated
        :param timeout: the max amount of time for which one execution of this task should be executed for. The execution
                        will be terminated if the runtime exceeds the given timeout (approximately).
        :param container_image: By default the configured FLYTE_INTERNAL_IMAGE is used for every task. This directive can be
                    used to provide an alternate image for a specific task. This is useful for the cases in which images
                    bloat because of various dependencies and a dependency is only required for this or a set of tasks,
                    and they vary from the default.

                    .. code-block:: python

                        # Use default image name `fqn` and alter the tag to `tag-{{default.tag}}` tag of the default image
                        # with a prefix. In this case, it is assumed that the image like
                        # flytecookbook:tag-gitsha is published alongwith the default of flytecookbook:gitsha
                        @task(container_image='{{.images.default.fqn}}:tag-{{images.default.tag}}')
                        def foo():
                            ...

                        # Refer to configurations to configure fqns for other images besides default. In this case it will
                        # lookup for an image named xyz
                        @task(container_image='{{.images.xyz.fqn}}:{{images.default.tag}}')
                        def foo2():
                            ...
        :param environment: Environment variables that should be added for this tasks execution
        :param requests: Specify compute resource requests for your task. For Pod-plugin tasks, these values will apply only
        to the primary container.
        :param limits: Compute limits. Specify compute resource limits for your task. For Pod-plugin tasks, these values
        will apply only to the primary container. For more information, please see :py:class:`flytekit.Resources`.
        :param secret_requests: Keys that can identify the secrets supplied at runtime. Ideally the secret keys should also be
                        semi-descriptive. The key values will be available from runtime, if the backend is configured
                        to provide secrets and if secrets are available in the configured secrets store.
                        Possible options for secret stores are - Vault, Confidant, Kube secrets, AWS KMS etc
                        Refer to :py:class:`Secret` to understand how to specify the request for a secret. It
                        may change based on the backend provider.
        :param execution_mode: This is mainly for internal use. Please ignore. It is filled in automatically.
        :param node_dependency_hints: A list of tasks, launchplans, or workflows that this task depends on. This is only
            for dynamic tasks/workflows, where flyte cannot automatically determine the dependencies prior to runtime.
            Even on dynamic tasks this is optional, but in some scenarios it will make registering the workflow easier,
            because it allows registration to be done the same as for static tasks/workflows.

            For example this is useful to run launchplans dynamically, because launchplans must be registered on flyteadmin
            before they can be run. Tasks and workflows do not have this requirement.

            .. code-block:: python

                @workflow
                def workflow0():
                    ...

                launchplan0 = LaunchPlan.get_or_create(workflow0)

                # Specify node_dependency_hints so that launchplan0 will be registered on flyteadmin, despite this being a
                # dynamic task.
                @dynamic(node_dependency_hints=[launchplan0])
                def launch_dynamically():
                    # To run a sub-launchplan it must have previously been registered on flyteadmin.
                    return [launchplan0]*10
        :param task_resolver: Provide a custom task resolver.
        :param disable_deck: (deprecated) If true, this task will not output deck html file
        :param enable_deck: If true, this task will output deck html file
        :param docs: Documentation about this task
        :param pod_template: Custom PodTemplate for this task.
        :param pod_template_name: The name of the existing PodTemplate resource which will be used in this task.
        :param accelerator: The accelerator to use for this task.
        """


        def wrapper(fn: Callable[..., Any]) -> PythonFunctionTask[T]:
            python_task = task(
                _task_function=fn,
                task_config=task_config,
                cache=cache,
                cache_serialize=cache_serialize,
                cache_version=cache_version,
                cache_ignore_input_vars=cache_ignore_input_vars,
                retries=retries,
                interruptible=interruptible,
                deprecated=deprecated,
                timeout=timeout,
                container_image=container_image,
                environment=environment,
                requests=requests,
                limits=limits,
                secret_requests=secret_requests,
                execution_mode=execution_mode,
                node_dependency_hints=node_dependency_hints,
                task_resolver=task_resolver,
                docs=docs,
                disable_deck=disable_deck,
                enable_deck=enable_deck,
                pod_template=pod_template,
                pod_template_name=pod_template_name,
                accelerator=accelerator
            )
            remote_python_task = RemotePythonTask(self.remote_entry, python_task)

            update_wrapper(remote_python_task, fn)
            return remote_python_task

            # _metadata = TaskMetadata(
            #     cache=cache,
            #     cache_serialize=cache_serialize,
            #     cache_version=cache_version,
            #     cache_ignore_input_vars=cache_ignore_input_vars,
            #     retries=retries,
            #     interruptible=interruptible,
            #     deprecated=deprecated,
            #     timeout=timeout,
            # )
            
            # task_instance = TaskPlugins.find_pythontask_plugin(type(task_config))(
            #     task_config,
            #     fn,
            #     metadata=_metadata,
            #     container_image=container_image,
            #     environment=environment,
            #     requests=requests,
            #     limits=limits,
            #     secret_requests=secret_requests,
            #     execution_mode=execution_mode,
            #     node_dependency_hints=node_dependency_hints,
            #     task_resolver=task_resolver,
            #     disable_deck=disable_deck,
            #     enable_deck=enable_deck,
            #     docs=docs,
            #     pod_template=pod_template,
            #     pod_template_name=pod_template_name,
            #     accelerator=accelerator,
            # )
            # update_wrapper(task_instance, fn)
            # return task_instance
        
        if _task_function is not None:
            return wrapper(_task_function)
        else:
            return wrapper
    
    def __init__(
        self,
        config: Config,
        default_project: Optional[str] = None,
        default_domain: Optional[str] = None,
        data_upload_location: str = "flyte://my-s3-bucket/",
        **kwargs,
    )  -> Callable[..., Callable[..., Callable[..., Any]]]:
        self.config = config
        self.default_project = default_project
        self.default_domain = default_domain
        self.data_upload_location = data_upload_location
        self.remote_entry = FlyteRemote(
            config=config, 
            default_project=default_project,
            default_domain=default_domain,
            data_upload_location=data_upload_location,
            **kwargs
        )

    def get_task_decorator(self):
        F = TypeVar('F', bound=Callable)
        def retain_type_hint(func: F) -> F:
            return func
        return retain_type_hint(self.task_decorator)