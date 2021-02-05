import datetime as _datetime
import inspect
from typing import Any, Callable, Dict, Optional, Type, Union

from flytekit.core.base_task import TaskMetadata
from flytekit.core.interface import transform_signature_to_interface
from flytekit.core.python_function_task import PythonFunctionTask
from flytekit.core.reference_entity import ReferenceEntity, TaskReference


class TaskPlugins(object):
    """
    This is the TaskPlugins factory for task types that are derivative of PythonFunctionTask.
    Every task that the user wishes to use should be available in this factory.
     Usage

    .. code-block:: python

        TaskPlugins.register_pythontask_plugin(config_object_type, plugin_object_type)
        # config_object_type is any class that will be passed to the plugin_object as task_config
        # Plugin_object_type is a derivative of ``PythonFunctionTask``
    """

    _PYTHONFUNCTION_TASK_PLUGINS: Dict[type, Type[PythonFunctionTask]] = {}

    @classmethod
    def register_pythontask_plugin(cls, plugin_config_type: type, plugin: Type[PythonFunctionTask]):
        """
        Use this method to register a new plugin into Flytekit
          Usage
        .. code-block:: python
            TaskPlugins.register_pythontask_plugin(config_object_type, plugin_object_type)
            # config_object_type is any class that will be passed to the plugin_object as task_config
            # Plugin_object_type is a derivative of ``PythonFunctionTask``
        """
        if plugin_config_type in cls._PYTHONFUNCTION_TASK_PLUGINS:
            found = cls._PYTHONFUNCTION_TASK_PLUGINS[plugin_config_type]
            if found == plugin:
                return
            raise TypeError(
                f"Requesting to register plugin {plugin} - collides with existing plugin {found}"
                f" for type {plugin_config_type}"
            )

        cls._PYTHONFUNCTION_TASK_PLUGINS[plugin_config_type] = plugin

    @classmethod
    def find_pythontask_plugin(cls, plugin_config_type: type) -> Type[PythonFunctionTask]:
        """
        Returns a PluginObjectType if found or returns the base PythonFunctionTask
        """
        if plugin_config_type in cls._PYTHONFUNCTION_TASK_PLUGINS:
            return cls._PYTHONFUNCTION_TASK_PLUGINS[plugin_config_type]
        # Defaults to returning Base PythonFunctionTask
        return PythonFunctionTask


def task(
    _task_function: Optional[Callable] = None,
    task_config: Optional[Any] = None,
    cache: bool = False,
    cache_version: str = "",
    retries: int = 0,
    interruptable: bool = False,
    deprecated: str = "",
    timeout: Union[_datetime.timedelta, int] = 0,
    container_image: Optional[str] = None,
    environment: Optional[Dict[str, str]] = None,
    **kwargs,
) -> Union[Callable, PythonFunctionTask]:
    """
    This is the core decorator to use for any task type in FlyteKit.
    Usage: for a simple python task

        .. code-block:: python

            @task(retries=3)
            def my_task(x: int, y: typing.Dict[str, str]) -> str:
                pass

    Usage: for specific task types

        .. code-block:: python

            @task(task_config=Spark(), retries=3)
            def my_task(x: int, y: typing.Dict[str, str]) -> str:
                pass

    :param _task_function: This argument is implicitly passed and represents the decorated function
    :param task_config: This argument provides configuration for a specific task types.
                        Please refer to the plugins documentation for the right Object to use
    :param cache: Boolean that indicates if caching should be enabled
    :param cache_version: Version string to be used for the cached value
    :param retries: for retries=n; n > 0, on failures of this task, the task will be retried at-least n number of times.
    :param interruptable: Boolean that indicates that this task can be interrupted and/or scheduled on nodes
                          with lower QoS guarantees. This will directly reduce the `$`/`execution cost` associated,
                           at the cost of performance penalties due to potential interruptions
    :param deprecated: A string that can be used to provide a warning message for deprecated task. Absence / empty str
                       indicates that the task is active and not deprecated
    :param timeout: the max amount of time for which one execution of this task should be executed for. If the execution
                    will be terminated if the runtime exceeds the given timeout (approximately)
    :param container_image: By default the configured FLYTE_INTERNAL_IMAGE is used for every task. This directive can be
                used to provide an alternate image for a specific task. This is useful for the cases in which images
                bloat because of various dependencies and a dependency is only required for this or a set of tasks,
                and they vary from the default. E.g.
                Usage:
                .. code-block:: python
                    # Use default image name `fqn` and alter the tag to `tag-{{default.tag}}` tag of the default image
                    # with a prefix. In this case, it is assumed that the image like
                    #  flytecookbook:tag-gitsha is published alongwith the default of flytecookbook:gitsha
                    @task(container_image='{{.images.default.fqn}}:tag-{{images.default.tag}}')
                    def foo():
                        pass

                    # Refer to configurations to configure fqns for other images besides default. In this case it will
                    # lookup for an image named xyz
                    @task(container_image='{{.images.xyz.fqn}}:{{images.default.tag}}')
                    def foo2():
                        pass

    :param environment: Environment variables that should be added for this tasks execution
    :param kwargs: Additional Kwargs. Refer to specific task implementations to find supported keywords. Ideally
                    all additional configuration values should be part of the ``task_config``
    """

    def wrapper(fn) -> PythonFunctionTask:
        _metadata = TaskMetadata(
            cache=cache,
            cache_version=cache_version,
            retries=retries,
            interruptable=interruptable,
            deprecated=deprecated,
            timeout=timeout,
        )

        task_instance = TaskPlugins.find_pythontask_plugin(type(task_config))(
            task_config, fn, metadata=_metadata, container_image=container_image, environment=environment, **kwargs,
        )

        return task_instance

    if _task_function:
        return wrapper(_task_function)
    else:
        return wrapper


class ReferenceTask(ReferenceEntity, PythonFunctionTask):
    """
    This is a reference task, the body of the function passed in through the constructor will never be used, only the
    signature of the function will be. The signature should also match the signature of the task you're referencing,
    as stored by Flyte Admin, if not, workflows using this will break upon compilation.
    """

    def __init__(
        self, project: str, domain: str, name: str, version: str, inputs: Dict[str, Type], outputs: Dict[str, Type]
    ):
        super().__init__(TaskReference(project, domain, name, version), inputs, outputs)


def reference_task(
    project: str, domain: str, name: str, version: str,
) -> Callable[[Callable[..., Any]], ReferenceTask]:
    """
    A reference task is a pointer to a task that already exists on your Flyte installation. This
    object will not initiate a network call to Admin, which is why the user is asked to provide the expected interface.
    If at registration time the interface provided causes an issue with compilation, an error will be returned.
    """

    def wrapper(fn) -> ReferenceTask:
        interface = transform_signature_to_interface(inspect.signature(fn))
        return ReferenceTask(project, domain, name, version, interface.inputs, interface.outputs)

    return wrapper
