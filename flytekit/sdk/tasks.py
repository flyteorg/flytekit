import datetime as _datetime

import six as _six

from flytekit.common import constants as _common_constants
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.tasks import generic_spark_task as _sdk_generic_spark_task
from flytekit.common.tasks import hive_task as _sdk_hive_tasks
from flytekit.common.tasks import pytorch_task as _sdk_pytorch_tasks
from flytekit.common.tasks import sdk_dynamic as _sdk_dynamic
from flytekit.common.tasks import sdk_runnable as _sdk_runnable_tasks
from flytekit.common.tasks import sidecar_task as _sdk_sidecar_tasks
from flytekit.common.tasks import spark_task as _sdk_spark_tasks
from flytekit.common.tasks import task as _task
from flytekit.common.tasks import tensorflow_task as _sdk_tensorflow_tasks
from flytekit.common.types import helpers as _type_helpers
from flytekit.contrib.notebook import tasks as _nb_tasks
from flytekit.models import interface as _interface_model
from flytekit.sdk.spark_types import SparkType as _spark_type


def inputs(_task_template=None, **kwargs):
    """
    Decorator that provides input definitions to a decorated task definition.

    .. note::

        Certain tasks have special input behavior.  See comments on each task decorator for more information.

    .. code-block:: python

        @inputs(in1=Types.Integer, in2=[Types.String], in3=[[[Types.Integer]]])
        @outputs(out1=Types.Integer, out2=Types.String)
        @python_task
        def my_task(wf_params, in1, in2, out1, out2):
            pass

    :param flytekit.common.tasks.sdk_runnable.SdkRunnableTask _task_template: Do not declare directly.  This is the
        decorated task template.
    :param dict[Text,flytekit.common.types.base_sdk_types.FlyteSdkType] kwargs: Arbitrary keyword arguments for input
        name and type.
    :rtype: flytekit.common.tasks.sdk_runnable.SdkRunnableTask
    """

    def apply_inputs_wrapper(task):
        if not isinstance(task, _task.SdkTask):
            additional_msg = "Inputs can only be applied to a task. Did you forget the task decorator on method '{}.{}'?".format(
                task.__module__, task.__name__ if hasattr(task, "__name__") else "<unknown>",
            )
            raise _user_exceptions.FlyteTypeException(
                expected_type=_sdk_runnable_tasks.SdkRunnableTask,
                received_type=type(task),
                received_value=task,
                additional_msg=additional_msg,
            )
        for k, v in _six.iteritems(kwargs):
            kwargs[k] = _interface_model.Variable(
                _type_helpers.python_std_to_sdk_type(v).to_flyte_literal_type(), ""
            )  # TODO: Support descriptions

        task.add_inputs(kwargs)
        return task

    if _task_template is not None:
        return apply_inputs_wrapper(_task_template)
    else:
        return apply_inputs_wrapper


def outputs(_task_template=None, **kwargs):
    """
    Decorator that provides output definitions to a decorated task definition.

    .. note::

        Certain tasks have special output behavior.  See comments on each task decorator for more information.

    .. code-block:: python

        @outputs(out1=Types.Integer, out2=Types.String)
        @python_task
        def my_task(wf_params, out1, out2):
            out1.set(123)
            out2.set('hello world!')

    :param flytekit.common.tasks.sdk_runnable.SdkRunnableTask _task_template: Do not declare directly.  This is the
        decorated task template.
    :param dict[Text,flytekit.common.types.base_sdk_types.FlyteSdkType] kwargs: Arbitrary keyword arguments for input
        name and type.
    :rtype: flytekit.common.tasks.sdk_runnable.SdkRunnableTask
    """

    def apply_outputs_wrapper(task):
        if not isinstance(task, _sdk_runnable_tasks.SdkRunnableTask) and not isinstance(
            task, _nb_tasks.SdkNotebookTask
        ):
            additional_msg = "Outputs can only be applied to a task. Did you forget the task decorator on method '{}.{}'?".format(
                task.__module__, task.__name__ if hasattr(task, "__name__") else "<unknown>",
            )
            raise _user_exceptions.FlyteTypeException(
                expected_type=_sdk_runnable_tasks.SdkRunnableTask,
                received_type=type(task),
                received_value=task,
                additional_msg=additional_msg,
            )
        for k, v in _six.iteritems(kwargs):
            kwargs[k] = _interface_model.Variable(
                _type_helpers.python_std_to_sdk_type(v).to_flyte_literal_type(), ""
            )  # TODO: Support descriptions

        task.add_outputs(kwargs)
        return task

    if _task_template is not None:
        return apply_outputs_wrapper(_task_template)
    else:
        return apply_outputs_wrapper


def python_task(
    _task_function=None,
    cache_version="",
    retries=0,
    interruptible=None,
    deprecated="",
    storage_request=None,
    cpu_request=None,
    gpu_request=None,
    memory_request=None,
    storage_limit=None,
    cpu_limit=None,
    gpu_limit=None,
    memory_limit=None,
    cache=False,
    timeout=None,
    environment=None,
    cls=None,
):
    """
    Decorator to create a Python Task definition.  This task will run as a single unit of work on the platform.

    .. code-block:: python

        @inputs(int_list=[Types.Integer])
        @outputs(sum_of_list=Types.Integer
        @python_task
        def my_task(wf_params, int_list, sum_of_list):
            sum_of_list.set(sum(int_list))

    :param _task_function: this is the decorated method and shouldn't be declared explicitly.  The function must
        take a first argument, and then named arguments matching those defined in @inputs and @outputs.  No keyword
        arguments are allowed for wrapped task functions.

    :param Text cache_version: [optional] string representing logical version for discovery.  This field should be
        updated whenever the underlying algorithm changes.

        .. note::

            This argument is required to be a non-empty string if `cache` is True.

    :param int retries: [optional] integer determining number of times task can be retried on
        :py:exc:`flytekit.sdk.exceptions.RecoverableException` or transient platform failures.  Defaults
        to 0.

        .. note::

            If retries > 0, the task must be able to recover from any remote state created within the user code.  It is
            strongly recommended that tasks are written to be idempotent.

    :param bool interruptible: [optional] boolean describing if the task is interruptible.

    :param Text deprecated: [optional] string that should be provided if this task is deprecated.  The string
        will be logged as a warning so it should contain information regarding how to update to a newer task.

    :param Text storage_request: [optional] Kubernetes resource string for lower-bound of disk storage space
        for the task to run.  Default is set by platform-level configuration.

        .. note::

            This is currently not supported by the platform.

    :param Text cpu_request: [optional] Kubernetes resource string for lower-bound of cores for the task to execute.
        This can be set to a fractional portion of a CPU. Default is set by platform-level configuration.

        TODO: Add links to resource string documentation for Kubernetes

    :param Text gpu_request: [optional] Kubernetes resource string for lower-bound of desired GPUs.
        Default is set by platform-level configuration.

        TODO: Add links to resource string documentation for Kubernetes

    :param Text memory_request: [optional]  Kubernetes resource string for lower-bound of physical memory
        necessary for the task to execute.  Default is set by platform-level configuration.

        TODO: Add links to resource string documentation for Kubernetes

    :param Text storage_limit: [optional] Kubernetes resource string for upper-bound of disk storage space
        for the task to run.  This amount is not guaranteed!  If not specified, it is set equal to storage_request.

        .. note::

            This is currently not supported by the platform.

    :param Text cpu_limit: [optional] Kubernetes resource string for upper-bound of cores for the task to execute.
        This can be set to a fractional portion of a CPU. This amount is not guaranteed!  If not specified,
        it is set equal to cpu_request.

    :param Text gpu_limit: [optional] Kubernetes resource string for upper-bound of desired GPUs. This amount is not
        guaranteed!  If not specified, it is set equal to gpu_request.

    :param Text memory_limit: [optional]  Kubernetes resource string for upper-bound of physical memory
        necessary for the task to execute.  This amount is not guaranteed!  If not specified, it is set equal to
        memory_request.

    :param bool cache: [optional] boolean describing if the outputs of this task should be cached and
        re-usable.

    :param datetime.timedelta timeout: [optional] describes how long the task should be allowed to
        run at max before triggering a retry (if retries are enabled).  By default, tasks are allowed to run
        indefinitely.  If a null timedelta is passed (i.e. timedelta(seconds=0)), the task will not timeout.

    :param dict[Text,Text] environment: [optional] environment variables to set when executing this task.

    :param cls: This can be used to override the task implementation with a user-defined extension. The class
        provided must be a subclass of flytekit.common.tasks.sdk_runnable.SdkRunnableTask.  A user can use this to
        inject bespoke logic into the base Flyte programming model.

    :rtype: flytekit.common.tasks.sdk_runnable.SdkRunnableTask
    """

    def wrapper(fn):
        return (cls or _sdk_runnable_tasks.SdkRunnableTask)(
            task_function=fn,
            task_type=_common_constants.SdkTaskType.PYTHON_TASK,
            discovery_version=cache_version,
            retries=retries,
            interruptible=interruptible,
            deprecated=deprecated,
            storage_request=storage_request,
            cpu_request=cpu_request,
            gpu_request=gpu_request,
            memory_request=memory_request,
            storage_limit=storage_limit,
            cpu_limit=cpu_limit,
            gpu_limit=gpu_limit,
            memory_limit=memory_limit,
            discoverable=cache,
            timeout=timeout or _datetime.timedelta(seconds=0),
            environment=environment,
            custom={},
        )

    if _task_function:
        return wrapper(_task_function)
    else:
        return wrapper


def dynamic_task(
    _task_function=None,
    cache_version="",
    retries=0,
    interruptible=None,
    deprecated="",
    storage_request=None,
    cpu_request=None,
    gpu_request=None,
    memory_request=None,
    storage_limit=None,
    cpu_limit=None,
    gpu_limit=None,
    memory_limit=None,
    cache=False,
    timeout=None,
    allowed_failure_ratio=None,
    max_concurrency=None,
    environment=None,
    cls=None,
):
    """
    Decorator to create a custom dynamic task definition.  Dynamic tasks should be used to split up work into
    an arbitrary number of parallel sub-tasks, or workflows.

    .. code-block:: python

        @outputs(out=Types.Integer)
        @python_task
        def my_sub_task(wf_params, out):
            out.set(randint())

        @outputs(out=[Types.Integer])
        @dynamic_task
        def my_task(wf_params, out):
            out_list = []
            for i in xrange(100):
                out_list.append(my_sub_task().outputs.out)
            out.set(out_list)

    .. note::

        All outputs of a batch task must be a list.  This is because the individual outputs of sub-tasks should be
        appended into a list.  There cannot be aggregation of outputs done in this task.  To accomplish aggregation,
        it is recommended that a python_task take the outputs of this task as input and do the necessary work.
        If a sub-task does not contribute an output, it must be yielded from the task with the `yield` keyword or
        returned from the task in a list.  If this isn't done, the sub-task will not be executed.

    :param _task_function: this is the decorated method and shouldn't be declared explicitly.  The function must
        take a first argument, and then named arguments matching those defined in @inputs and @outputs.  No keyword
        arguments are allowed.
    :param Text cache_version: [optional] string representing logical version for discovery.  This field should be
        updated whenever the underlying algorithm changes.

        .. note::

            This argument is required to be a non-empty string if `cache` is True.

    :param int retries: [optional] integer determining number of times task can be retried on
        :py:exc:`flytekit.sdk.exceptions.RecoverableException` or transient platform failures.  Defaults
        to 0.

        .. note::

            If retries > 0, the task must be able to recover from any remote state created within the user code.  It is
            strongly recommended that tasks are written to be idempotent.

    :param bool interruptible: [optional] boolean describing if the task is interruptible.

    :param Text deprecated: [optional] string that should be provided if this task is deprecated.  The string
        will be logged as a warning so it should contain information regarding how to update to a newer task.
    :param Text storage_request: [optional] Kubernetes resource string for lower-bound of disk storage space
        for the task to run.  Default is set by platform-level configuration.

        .. note::

            This is currently not supported by the platform.

    :param Text cpu_request: [optional] Kubernetes resource string for lower-bound of cores for the task to execute.
        This can be set to a fractional portion of a CPU. Default is set by platform-level configuration.
        TODO: Add links to resource string documentation for Kubernetes
    :param Text gpu_request: [optional] Kubernetes resource string for lower-bound of desired GPUs.
        Default is set by platform-level configuration.
        TODO: Add links to resource string documentation for Kubernetes
    :param Text memory_request: [optional]  Kubernetes resource string for lower-bound of physical memory
        necessary for the task to execute.  Default is set by platform-level configuration.
        TODO: Add links to resource string documentation for Kubernetes
    :param Text storage_limit: [optional] Kubernetes resource string for upper-bound of disk storage space
        for the task to run.  This amount is not guaranteed!  If not specified, it is set equal to storage_request.

        .. note::

            This is currently not supported by the platform.

    :param Text cpu_limit: [optional] Kubernetes resource string for upper-bound of cores for the task to execute.
        This can be set to a fractional portion of a CPU. This amount is not guaranteed!  If not specified,
        it is set equal to cpu_request.
    :param Text gpu_limit: [optional] Kubernetes resource string for upper-bound of desired GPUs. This amount is not
        guaranteed!  If not specified, it is set equal to gpu_request.
    :param Text memory_limit: [optional]  Kubernetes resource string for upper-bound of physical memory
        necessary for the task to execute.  This amount is not guaranteed!  If not specified, it is set equal to
        memory_request.
    :param bool cache: [optional] boolean describing if the outputs of this task should be cached and
        re-usable.
    :param datetime.timedelta timeout: [optional] describes how long the task should be allowed to
        run at max before triggering a retry (if retries are enabled).  By default, tasks are allowed to run
        indefinitely.  If a null timedelta is passed (i.e. timedelta(seconds=0)), the task will not timeout.
    :param float allowed_failure_ratio: [optional] float value describing the ratio of sub-tasks that may fail before
        the master batch task considers itself a failure.  By default, the value is 0 so if any task fails, the master
        batch task will be marked a failure.  If specified, the value must be between 0 and 1 inclusive.  In the event a
        non-zero value is specified, downstream tasks must be able to accept None values as outputs from individual
        sub-tasks because the output values will be set to None for any sub-task that fails.
    :param int max_concurrency: [optional] integer value describing the maximum number of tasks to run concurrently.
        This is a stand-in pending better concurrency controls for special use-cases.  The existence of this parameter
        is not guaranteed between versions and therefore it is NOT recommended that it be used.
    :param dict[Text,Text] environment: [optional] environment variables to set when executing this task.
    :param cls: This can be used to override the task implementation with a user-defined extension. The class
        provided must be a subclass of flytekit.common.tasks.sdk_runnable.SdkRunnableTask.  Generally, it should be a
        subclass of flytekit.common.tasks.sdk_dynamic.SdkDynamicTask.  A user can use this parameter to inject bespoke
        logic into the base Flyte programming model.
    :rtype: flytekit.common.tasks.sdk_runnable.SdkDynamicTask
    """

    def wrapper(fn):
        return (cls or _sdk_dynamic.SdkDynamicTask)(
            task_function=fn,
            task_type=_common_constants.SdkTaskType.DYNAMIC_TASK,
            discovery_version=cache_version,
            retries=retries,
            interruptible=interruptible,
            deprecated=deprecated,
            storage_request=storage_request,
            cpu_request=cpu_request,
            gpu_request=gpu_request,
            memory_request=memory_request,
            storage_limit=storage_limit,
            cpu_limit=cpu_limit,
            gpu_limit=gpu_limit,
            memory_limit=memory_limit,
            discoverable=cache,
            timeout=timeout or _datetime.timedelta(seconds=0),
            allowed_failure_ratio=allowed_failure_ratio,
            max_concurrency=max_concurrency,
            environment=environment or {},
            custom={},
        )

    if _task_function:
        return wrapper(_task_function)
    else:
        return wrapper


def spark_task(
    _task_function=None,
    cache_version="",
    retries=0,
    interruptible=None,
    deprecated="",
    cache=False,
    timeout=None,
    spark_conf=None,
    hadoop_conf=None,
    environment=None,
    cls=None,
):
    """
    Decorator to create a spark task.  This task will connect to a Spark cluster, configure the environment,
    and then execute the code within the _task_function as the Spark driver program.

    .. code-block:: python

        @inputs(a=Types.Integer)
        @spark_task(
            spark_conf={
                    'spark.executor.cores': '7',
                    'spark.executor.instances': '31',
                    'spark.executor.memory': '32G'
                }
            )
        def sparky(wf_params, spark_context, a):
            pass

    :param _task_function: this is the decorated method and shouldn't be declared explicitly.  The function must
        take a first argument, and then named arguments matching those defined in @inputs and @outputs.  No keyword
        arguments are allowed for wrapped task functions.
    :param Text cache_version: [optional] string representing logical version for discovery.  This field should be
        updated whenever the underlying algorithm changes.

        .. note::

            This argument is required to be a non-empty string if `cache` is True.

    :param int retries: [optional] integer determining number of times task can be retried on
        :py:exc:`flytekit.sdk.exceptions.RecoverableException` or transient platform failures.  Defaults
        to 0.

        .. note::

            If retries > 0, the task must be able to recover from any remote state created within the user code.  It is
            strongly recommended that tasks are written to be idempotent.

    :param Text deprecated: [optional] string that should be provided if this task is deprecated.  The string
        will be logged as a warning so it should contain information regarding how to update to a newer task.
    :param bool cache: [optional] boolean describing if the outputs of this task should be cached and
        re-usable.
    :param datetime.timedelta timeout: [optional] describes how long the task should be allowed to
        run at max before triggering a retry (if retries are enabled).  By default, tasks are allowed to run
        indefinitely.  If a null timedelta is passed (i.e. timedelta(seconds=0)), the task will not timeout.
    :param dict[Text,Text] spark_conf: A definition of key-value pairs for spark config for the job.
    :param dict[Text,Text] hadoop_conf: A definition of key-value pairs for hadoop config for the job.
    :param dict[Text,Text] environment: [optional] environment variables to set when executing this task.
    :param cls: This can be used to override the task implementation with a user-defined extension. The class
        provided must be a subclass of flytekit.common.tasks.sdk_runnable.SdkRunnableTask.  Generally, it should be a
        subclass of flytekit.common.tasks.spark_task.SdkSparkTask.  A user can use this parameter to inject bespoke
        logic into the base Flyte programming model.
    :rtype: flytekit.common.tasks.sdk_runnable.SdkRunnableTask
    """

    def wrapper(fn):
        return (cls or _sdk_spark_tasks.SdkSparkTask)(
            task_function=fn,
            task_type=_common_constants.SdkTaskType.SPARK_TASK,
            discovery_version=cache_version,
            retries=retries,
            interruptible=interruptible,
            spark_type=_spark_type.PYTHON,
            deprecated=deprecated,
            discoverable=cache,
            timeout=timeout or _datetime.timedelta(seconds=0),
            spark_conf=spark_conf or {},
            hadoop_conf=hadoop_conf or {},
            environment=environment or {},
        )

    if _task_function:
        return wrapper(_task_function)
    else:
        return wrapper


def generic_spark_task(
    spark_type,
    main_class,
    main_application_file,
    cache_version="",
    retries=0,
    interruptible=None,
    inputs=None,
    deprecated="",
    cache=False,
    timeout=None,
    spark_conf=None,
    hadoop_conf=None,
    environment=None,
):
    """
    Create a generic spark task. This task will connect to a Spark cluster, configure the environment,
    and then execute the mainClass code as the Spark driver program.

    """

    return _sdk_generic_spark_task.SdkGenericSparkTask(
        task_type=_common_constants.SdkTaskType.SPARK_TASK,
        discovery_version=cache_version,
        retries=retries,
        interruptible=interruptible,
        deprecated=deprecated,
        discoverable=cache,
        timeout=timeout or _datetime.timedelta(seconds=0),
        spark_type=spark_type,
        task_inputs=inputs,
        main_class=main_class or "",
        main_application_file=main_application_file or "",
        spark_conf=spark_conf or {},
        hadoop_conf=hadoop_conf or {},
        environment=environment or {},
    )


def qubole_spark_task(*args, **kwargs):
    """
    :rtype: flytekit.common.tasks.sdk_runnable.SdkRunnableTask
    """
    raise NotImplementedError("Qubole Spark Task is currently not supported in Flyte.")


def hive_task(
    _task_function=None,
    cache_version="",
    retries=0,
    interruptible=None,
    deprecated="",
    storage_request=None,
    cpu_request=None,
    gpu_request=None,
    memory_request=None,
    storage_limit=None,
    cpu_limit=None,
    gpu_limit=None,
    memory_limit=None,
    cache=False,
    timeout=None,
    environment=None,
    cls=None,
):
    """
    Decorator to create a hive task. This task should output a list of hive queries which are run on a hive cluster.

    This is a 2 step task:

        1. Generator step runs in the user container and outputs a list of queries.
        2. The list of queries produced in step1 is then submitted to Hive Cluster. The queries are monitored by Flyte
           Backend for completion.
    Container properties(cpu, gpu, memory, etc) set on this task are only used in step1 above.

    .. code-block:: python

        @inputs(a=Types.Integer)
        @hive_task(
            cache_version='1',
        )
        def test_hive(wf_params, a):
            return [
                "SELECT * FROM users_table WHERE user_id=4",
                "INSERT INTO users_table VALUES ("user", 5, 4)"
            ]

     :param _task_function: this is the decorated method and shouldn't be declared explicitly.  The function must
        take a first argument, and then named arguments matching those defined in @inputs and @outputs.  No keyword
        arguments are allowed for wrapped task functions.

    :param Text cache_version: [optional] string representing logical version for discovery.  This field should be
        updated whenever the underlying algorithm changes.

        .. note::

            This argument is required to be a non-empty string if `cache` is True.

    :param int retries: [optional] integer determining number of times task can be retried on
        :py:exc:`flytekit.common.exceptions.RecoverableException` or transient platform failures.  Defaults
        to 0.

        .. note::

            If retries > 0, the task must be able to recover from any remote state created within the user code.  It is
            strongly recommended that tasks are written to be idempotent.

    :param bool interruptible: [optional] boolean describing if task is interruptible.
    :param Text deprecated: [optional] string that should be provided if this task is deprecated.  The string
        will be logged as a warning so it should contain information regarding how to update to a newer task.
    :param Text storage_request: [optional] Kubernetes resource string for lower-bound of disk storage space
        for the task to run.  Default is set by platform-level configuration.

        .. note::

            This is currently not supported by the platform.

    :param Text cpu_request: [optional] Kubernetes resource string for lower-bound of cores for the task to execute.
        This can be set to a fractional portion of a CPU. Default is set by platform-level configuration.
        TODO: Add links to resource string documentation for Kubernetes
    :param Text gpu_request: [optional] Kubernetes resource string for lower-bound of desired GPUs.
        Default is set by platform-level configuration.
        TODO: Add links to resource string documentation for Kubernetes
    :param Text memory_request: [optional]  Kubernetes resource string for lower-bound of physical memory
        necessary for the task to execute.  Default is set by platform-level configuration.
        TODO: Add links to resource string documentation for Kubernetes
    :param Text storage_limit: [optional] Kubernetes resource string for upper-bound of disk storage space
        for the task to run.  This amount is not guaranteed!  If not specified, it is set equal to storage_request.

        .. note::

            This is currently not supported by the platform.

    :param Text cpu_limit: [optional] Kubernetes resource string for upper-bound of cores for the task to execute.
        This can be set to a fractional portion of a CPU. This amount is not guaranteed!  If not specified,
        it is set equal to cpu_request.
    :param Text gpu_limit: [optional] Kubernetes resource string for upper-bound of desired GPUs. This amount is not
        guaranteed!  If not specified, it is set equal to gpu_request.
    :param Text memory_limit: [optional]  Kubernetes resource string for upper-bound of physical memory
        necessary for the task to execute.  This amount is not guaranteed!  If not specified, it is set equal to
        memory_request.
    :param bool cache: [optional] boolean describing if the outputs of this task should be cached and
        re-usable.
    :param datetime.timedelta timeout: [optional] describes how long the task should be allowed to
        run at max before triggering a retry (if retries are enabled).  By default, tasks are allowed to run
        indefinitely.  If a null timedelta is passed (i.e. timedelta(seconds=0)), the task will not timeout.
    :param dict[Text,Text] environment: Environment variables to set for the execution of the query-generating
        container.
    :param cls: This can be used to override the task implementation with a user-defined extension. The class
        provided should be a subclass of flytekit.common.tasks.sdk_runnable.SdkRunnableTask. Generally, it should be
        a subclass of flytekit.common.tasks.hive_task.SdkHiveTask. A user can use this to inject bespoke logic into
        the base Flyte programming model.

    :rtype: flytekit.common.tasks.sdk_runnable.SdkHiveTask
    """

    def wrapper(fn):

        return (cls or _sdk_hive_tasks.SdkHiveTask)(
            task_function=fn,
            task_type=_common_constants.SdkTaskType.BATCH_HIVE_TASK,
            discovery_version=cache_version,
            retries=retries,
            interruptible=interruptible,
            deprecated=deprecated,
            storage_request=storage_request,
            cpu_request=cpu_request,
            gpu_request=gpu_request,
            memory_request=memory_request,
            storage_limit=storage_limit,
            cpu_limit=cpu_limit,
            gpu_limit=gpu_limit,
            memory_limit=memory_limit,
            discoverable=cache,
            timeout=timeout or _datetime.timedelta(seconds=0),
            cluster_label="",
            tags=[],
            environment=environment or {},
        )

    if _task_function:
        return wrapper(_task_function)
    else:
        return wrapper


def qubole_hive_task(
    _task_function=None,
    cache_version="",
    retries=0,
    interruptible=None,
    deprecated="",
    storage_request=None,
    cpu_request=None,
    gpu_request=None,
    memory_request=None,
    storage_limit=None,
    cpu_limit=None,
    gpu_limit=None,
    memory_limit=None,
    cache=False,
    timeout=None,
    cluster_label=None,
    tags=None,
    environment=None,
    cls=None,
):
    """
    Decorator to create a qubole hive task. This is hive task runs on a qubole cluster, and therefore allows users to
    pass cluster labels and qubole query tags. Similar to hive task, this task should output a list of hive queries
    that are run on a hive cluster.

    Similar to a hive task, this is also a 2 step task where step2 is run on a qubole hive cluster. Therefore, users can
    specify qubole cluster_label and query tags on this task.

    .. code-block:: python

        @inputs(a=Types.Integer)
        @qubole_hive_task(
            cache_version='1',
            cluster_label='cluster_label',
            tags=['tag1'],
            )
        def test_hive(wf_params, a):
            return [
                "SELECT * FROM users_table WHERE user_id=4",
                "INSERT INTO users_table VALUES ("user", 5, 4)"
            ]

     :param _task_function: this is the decorated method and shouldn't be declared explicitly.  The function must
        take a first argument, and then named arguments matching those defined in @inputs and @outputs.  No keyword
        arguments are allowed for wrapped task functions.

    :param Text cache_version: [optional] string representing logical version for discovery.  This field should be
        updated whenever the underlying algorithm changes.

        .. note::

            This argument is required to be a non-empty string if `cache` is True.

    :param int retries: [optional] integer determining number of times task can be retried on
        :py:exc:`flytekit.common.exceptions.RecoverableException` or transient platform failures.  Defaults
        to 0.

        .. note::

            If retries > 0, the task must be able to recover from any remote state created within the user code.  It is
            strongly recommended that tasks are written to be idempotent.

    :param bool interruptible: [optional] boolean describing if task is interruptible.
    :param Text deprecated: [optional] string that should be provided if this task is deprecated.  The string
        will be logged as a warning so it should contain information regarding how to update to a newer task.
    :param Text storage_request: [optional] Kubernetes resource string for lower-bound of disk storage space
        for the task to run.  Default is set by platform-level configuration.

        .. note::

            This is currently not supported by the platform.

    :param Text cpu_request: [optional] Kubernetes resource string for lower-bound of cores for the task to execute.
        This can be set to a fractional portion of a CPU. Default is set by platform-level configuration.
        TODO: Add links to resource string documentation for Kubernetes
    :param Text gpu_request: [optional] Kubernetes resource string for lower-bound of desired GPUs.
        Default is set by platform-level configuration.
        TODO: Add links to resource string documentation for Kubernetes
    :param Text memory_request: [optional]  Kubernetes resource string for lower-bound of physical memory
        necessary for the task to execute.  Default is set by platform-level configuration.
        TODO: Add links to resource string documentation for Kubernetes
    :param Text storage_limit: [optional] Kubernetes resource string for upper-bound of disk storage space
        for the task to run.  This amount is not guaranteed!  If not specified, it is set equal to storage_request.

        .. note::

            This is currently not supported by the platform.

    :param Text cpu_limit: [optional] Kubernetes resource string for upper-bound of cores for the task to execute.
        This can be set to a fractional portion of a CPU. This amount is not guaranteed!  If not specified,
        it is set equal to cpu_request.
    :param Text gpu_limit: [optional] Kubernetes resource string for upper-bound of desired GPUs. This amount is not
        guaranteed!  If not specified, it is set equal to gpu_request.
    :param Text memory_limit: [optional]  Kubernetes resource string for upper-bound of physical memory
        necessary for the task to execute.  This amount is not guaranteed!  If not specified, it is set equal to
        memory_request.
    :param bool cache: [optional] boolean describing if the outputs of this task should be cached and
        re-usable.
    :param datetime.timedelta timeout: [optional] describes how long the task should be allowed to
        run at max before triggering a retry (if retries are enabled).  By default, tasks are allowed to run
        indefinitely.  If a null timedelta is passed (i.e. timedelta(seconds=0)), the task will not timeout.
    :param cluster_label: The qubole cluster label where the query is to be executed
    :param list[Text] tags: User defined tags(key-value pairs) defined by the user for the queries. These tags are
        passed to Qubole.
    :param dict[Text,Text] environment: Environment variables to set for the execution of the query-generating
        container.
    :param cls: This can be used to override the task implementation with a user-defined extension. The class
        provided should be a subclass of flytekit.common.tasks.sdk_runnable.SdkRunnableTask. Generally, it should be
        a subclass of flytekit.common.tasks.hive_task.SdkHiveTask. A user can use this to inject bespoke logic into
        the base Flyte programming model.

    :rtype: flytekit.common.tasks.sdk_runnable.SdkHiveTask
    """

    def wrapper(fn):

        return (cls or _sdk_hive_tasks.SdkHiveTask)(
            task_function=fn,
            task_type=_common_constants.SdkTaskType.BATCH_HIVE_TASK,
            discovery_version=cache_version,
            retries=retries,
            interruptible=interruptible,
            deprecated=deprecated,
            storage_request=storage_request,
            cpu_request=cpu_request,
            gpu_request=gpu_request,
            memory_request=memory_request,
            storage_limit=storage_limit,
            cpu_limit=cpu_limit,
            gpu_limit=gpu_limit,
            memory_limit=memory_limit,
            discoverable=cache,
            timeout=timeout or _datetime.timedelta(seconds=0),
            cluster_label=cluster_label or "",
            tags=tags or [],
            environment=environment or {},
        )

    # This is syntactic-sugar, so that when calling this decorator without args, you can either
    # do it with () or without any ()
    if _task_function:
        return wrapper(_task_function)
    else:
        return wrapper


def sidecar_task(
    _task_function=None,
    cache_version="",
    retries=0,
    interruptible=None,
    deprecated="",
    storage_request=None,
    cpu_request=None,
    gpu_request=None,
    memory_request=None,
    storage_limit=None,
    cpu_limit=None,
    gpu_limit=None,
    memory_limit=None,
    cache=False,
    timeout=None,
    environment=None,
    pod_spec=None,
    primary_container_name=None,
    cls=None,
):
    """
    Decorator to create a Sidecar Task definition.  This task will execute the primary task alongside the specified
    kubernetes PodSpec. Custom primary task container attributes can be defined in the PodSpec by defining a container
    whose name matches the primary_container_name. These container attributes will be applied to the container brought
    up to execute the primary task definition.

    .. code-block:: python

        def generate_pod_spec_for_task():
            pod_spec = generated_pb2.PodSpec()
            secondary_container = generated_pb2.Container(
                name="secondary",
                image="alpine",
            )
            secondary_container.command.extend(["/bin/sh"])
            secondary_container.args.extend(["-c", "echo hi sidecar world > /data/message.txt"])
            shared_volume_mount = generated_pb2.VolumeMount(
                name="shared-data",
                mountPath="/data",
            )
            secondary_container.volumeMounts.extend([shared_volume_mount])

            primary_container = generated_pb2.Container(name="primary")
            primary_container.volumeMounts.extend([shared_volume_mount])

            pod_spec.volumes.extend([generated_pb2.Volume(
                name="shared-data",
                volumeSource=generated_pb2.VolumeSource(
                    emptyDir=generated_pb2.EmptyDirVolumeSource(
                        medium="Memory",
                    )
                )
            )])
            pod_spec.containers.extend([primary_container, secondary_container])
            return pod_spec

        @sidecar_task(
            pod_spec=generate_pod_spec_for_task(),
            primary_container_name="primary",
        )
        def a_sidecar_task(wfparams):
            while not os.path.isfile('/data/message.txt'):
                time.sleep(5)


    :param _task_function: this is the decorated method and shouldn't be declared explicitly.  The function must
        take a first argument, and then named arguments matching those defined in @inputs and @outputs.  No keyword
        arguments are allowed for wrapped task functions.

    :param Text cache_version: [optional] string representing logical version for discovery.  This field should be
        updated whenever the underlying algorithm changes.
        .. note::
            This argument is required to be a non-empty string if `cache` is True.

    :param int retries: [optional] integer determining number of times task can be retried on
        :py:ex:`flytekit.sdk.exceptions.RecoverableException` or transient platform failures.  Defaults
        to 0.

        .. note::

            If retries > 0, the task must be able to recover from any remote state created within the user code.  It is
            strongly recommended that tasks are written to be idempotent.

    :param bool interruptible: Specify whether task is interruptible

    :param Text deprecated: [optional] string that should be provided if this task is deprecated.  The string
        will be logged as a warning so it should contain information regarding how to update to a newer task.

    :param Text storage_request: [optional] Kubernetes resource string for lower-bound of disk storage space
        for the task to run.  Default is set by platform-level configuration.

        TODO: !!!
        .. note::

            This is currently not supported by the platform.

    :param Text cpu_request: [optional] Kubernetes resource string for lower-bound of cores for the task to execute.
        This can be set to a fractional portion of a CPU. Default is set by platform-level configuration.

        TODO: Add links to resource string documentation for Kubernetes

    :param Text gpu_request: [optional] Kubernetes resource string for lower-bound of desired GPUs.
        Default is set by platform-level configuration.

        TODO: Add links to resource string documentation for Kubernetes

    :param Text memory_request: [optional]  Kubernetes resource string for lower-bound of physical memory
        necessary for the task to execute.  Default is set by platform-level configuration.

        TODO: Add links to resource string documentation for Kubernetes

    :param Text storage_limit: [optional] Kubernetes resource string for upper-bound of disk storage space
        for the task to run.  This amount is not guaranteed!  If not specified, it is set equal to storage_request.

        TODO: !!!
        .. note::

            This is currently not supported by the platform.

    :param Text cpu_limit: [optional] Kubernetes resource string for upper-bound of cores for the task to execute.
        This can be set to a fractional portion of a CPU. This amount is not guaranteed!  If not specified,
        it is set equal to cpu_request.

    :param Text gpu_limit: [optional] Kubernetes resource string for upper-bound of desired GPUs. This amount is not
        guaranteed!  If not specified, it is set equal to gpu_request.

    :param Text memory_limit: [optional]  Kubernetes resource string for upper-bound of physical memory
        necessary for the task to execute.  This amount is not guaranteed!  If not specified, it is set equal to
        memory_request.

    :param bool cache: [optional] boolean describing if the outputs of this task should be discoverable and
        re-usable.

    :param datetime.timedelta timeout: [optional] describes how long the task should be allowed to
        run at max before triggering a retry (if retries are enabled).  By default, tasks are allowed to run
        indefinitely.  If a null timedelta is passed (i.e. timedelta(seconds=0)), the task will not timeout.

    :param dict[Text,Text] environment: [optional] environment variables to set when executing this task.

    :param k8s.io.api.core.v1.generated_pb2.PodSpec pod_spec: [optional] PodSpec to bring up alongside task execution.

    :param Text primary_container_name: primary container to monitor for the duration of the task.

    :param cls: This can be used to override the task implementation with a user-defined extension. The class
        provided must be a subclass of flytekit.common.tasks.sdk_runnable.SdkRunnableTask.  A user can use this to
        inject bespoke logic into the base Flyte programming model.

    :rtype: flytekit.common.tasks.sdk_runnable.SdkRunnableTask

    """

    def wrapper(fn):

        return (cls or _sdk_sidecar_tasks.SdkSidecarTask)(
            task_function=fn,
            task_type=_common_constants.SdkTaskType.SIDECAR_TASK,
            discovery_version=cache_version,
            retries=retries,
            interruptible=interruptible,
            deprecated=deprecated,
            storage_request=storage_request,
            cpu_request=cpu_request,
            gpu_request=gpu_request,
            memory_request=memory_request,
            storage_limit=storage_limit,
            cpu_limit=cpu_limit,
            gpu_limit=gpu_limit,
            memory_limit=memory_limit,
            discoverable=cache,
            timeout=timeout or _datetime.timedelta(seconds=0),
            environment=environment,
            pod_spec=pod_spec,
            primary_container_name=primary_container_name,
        )

    if _task_function:
        return wrapper(_task_function)
    else:
        return wrapper


def dynamic_sidecar_task(
    _task_function=None,
    cache_version="",
    retries=0,
    interruptible=None,
    deprecated="",
    storage_request=None,
    cpu_request=None,
    gpu_request=None,
    memory_request=None,
    storage_limit=None,
    cpu_limit=None,
    gpu_limit=None,
    memory_limit=None,
    cache=False,
    timeout=None,
    allowed_failure_ratio=None,
    max_concurrency=None,
    environment=None,
    pod_spec=None,
    primary_container_name=None,
    cls=None,
):
    """
    Decorator to create a custom dynamic sidecar task definition. Dynamic
    tasks should be used to split up work into an arbitrary number of parallel
    sub-tasks, or workflows. This task will execute the primary task alongside
    the specified kubernetes PodSpec. Custom primary task container attributes
    can be defined in the PodSpec by defining a container whose name matches
    the primary_container_name. These container attributes will be applied to
    the container brought up to execute the primary task definition.
    .. code-block:: python
        def generate_pod_spec_for_task():
            pod_spec = generated_pb2.PodSpec()
            secondary_container = generated_pb2.Container(
                name="secondary",
                image="alpine",
            )
            secondary_container.command.extend(["/bin/sh"])
            secondary_container.args.extend(["-c", "echo hi sidecar world > /data/message.txt"])
            shared_volume_mount = generated_pb2.VolumeMount(
                name="shared-data",
                mountPath="/data",
            )
            secondary_container.volumeMounts.extend([shared_volume_mount])
            primary_container = generated_pb2.Container(name="primary")
            primary_container.volumeMounts.extend([shared_volume_mount])
            pod_spec.volumes.extend([generated_pb2.Volume(
                name="shared-data",
                volumeSource=generated_pb2.VolumeSource(
                    emptyDir=generated_pb2.EmptyDirVolumeSource(
                        medium="Memory",
                    )
                )
            )])
            pod_spec.containers.extend([primary_container, secondary_container])
            return pod_spec
        @outputs(out=Types.Integer)
        @python_task
        def my_sub_task(wf_params, out):
            out.set(randint())
        @outputs(out=[Types.Integer])
        @dynamic_sidecar_task(
            pod_spec=generate_pod_spec_for_task(),
            primary_container_name="primary",
        )
        def my_task(wf_params, out):
            out_list = []
            for i in xrange(100):
                out_list.append(my_sub_task().outputs.out)
            out.set(out_list)
    .. note::
        All outputs of a batch task must be a list.  This is because the individual outputs of sub-tasks should be
        appended into a list.  There cannot be aggregation of outputs done in this task.  To accomplish aggregation,
        it is recommended that a python_task take the outputs of this task as input and do the necessary work.
        If a sub-task does not contribute an output, it must be yielded from the task with the `yield` keyword or
        returned from the task in a list.  If this isn't done, the sub-task will not be executed.
    :param _task_function: this is the decorated method and shouldn't be declared explicitly.  The function must
        take a first argument, and then named arguments matching those defined in @inputs and @outputs.  No keyword
        arguments are allowed.
    :param Text cache_version: [optional] string representing logical version for discovery.  This field should be
        updated whenever the underlying algorithm changes.
        .. note::
            This argument is required to be a non-empty string if `cache` is True.
    :param int retries: [optional] integer determining number of times task can be retried on
        :py:exc:`flytekit.sdk.exceptions.RecoverableException` or transient platform failures.  Defaults
        to 0.
        .. note::
            If retries > 0, the task must be able to recover from any remote state created within the user code.  It is
            strongly recommended that tasks are written to be idempotent.
    :param bool interruptible: [optional] boolean describing if the task is interruptible.
    :param Text deprecated: [optional] string that should be provided if this task is deprecated.  The string
        will be logged as a warning so it should contain information regarding how to update to a newer task.
    :param Text storage_request: [optional] Kubernetes resource string for lower-bound of disk storage space
        for the task to run.  Default is set by platform-level configuration.
        .. note::
            This is currently not supported by the platform.
    :param Text cpu_request: [optional] Kubernetes resource string for lower-bound of cores for the task to execute.
        This can be set to a fractional portion of a CPU. Default is set by platform-level configuration.
        TODO: Add links to resource string documentation for Kubernetes
    :param Text gpu_request: [optional] Kubernetes resource string for lower-bound of desired GPUs.
        Default is set by platform-level configuration.
        TODO: Add links to resource string documentation for Kubernetes
    :param Text memory_request: [optional]  Kubernetes resource string for lower-bound of physical memory
        necessary for the task to execute.  Default is set by platform-level configuration.
        TODO: Add links to resource string documentation for Kubernetes
    :param Text storage_limit: [optional] Kubernetes resource string for upper-bound of disk storage space
        for the task to run.  This amount is not guaranteed!  If not specified, it is set equal to storage_request.
        .. note::
            This is currently not supported by the platform.
    :param Text cpu_limit: [optional] Kubernetes resource string for upper-bound of cores for the task to execute.
        This can be set to a fractional portion of a CPU. This amount is not guaranteed!  If not specified,
        it is set equal to cpu_request.
    :param Text gpu_limit: [optional] Kubernetes resource string for upper-bound of desired GPUs. This amount is not
        guaranteed!  If not specified, it is set equal to gpu_request.
    :param Text memory_limit: [optional]  Kubernetes resource string for upper-bound of physical memory
        necessary for the task to execute.  This amount is not guaranteed!  If not specified, it is set equal to
        memory_request.
    :param bool cache: [optional] boolean describing if the outputs of this task should be cached and
        re-usable.
    :param datetime.timedelta timeout: [optional] describes how long the task should be allowed to
        run at max before triggering a retry (if retries are enabled).  By default, tasks are allowed to run
        indefinitely.  If a null timedelta is passed (i.e. timedelta(seconds=0)), the task will not timeout.
    :param float allowed_failure_ratio: [optional] float value describing the ratio of sub-tasks that may fail before
        the master batch task considers itself a failure.  By default, the value is 0 so if any task fails, the master
        batch task will be marked a failure.  If specified, the value must be between 0 and 1 inclusive.  In the event a
        non-zero value is specified, downstream tasks must be able to accept None values as outputs from individual
        sub-tasks because the output values will be set to None for any sub-task that fails.
    :param int max_concurrency: [optional] integer value describing the maximum number of tasks to run concurrently.
        This is a stand-in pending better concurrency controls for special use-cases.  The existence of this parameter
        is not guaranteed between versions and therefore it is NOT recommended that it be used.
    :param dict[Text,Text] environment: [optional] environment variables to set when executing this task.
    :param k8s.io.api.core.v1.generated_pb2.PodSpec pod_spec: PodSpec to bring up alongside task execution.
    :param Text primary_container_name: primary container to monitor for the duration of the task.
    :param cls: This can be used to override the task implementation with a user-defined extension. The class
        provided must be a subclass of flytekit.common.tasks.sdk_runnable.SdkRunnableTask.  Generally, it should be a
        subclass of flytekit.common.tasks.sidecar_Task.SdkDynamicSidecarTask.  A user can use this parameter to inject bespoke
        logic into the base Flyte programming model.
    :rtype: flytekit.common.tasks.sidecar_Task.SdkDynamicSidecarTask
    """

    def wrapper(fn):
        return (cls or _sdk_sidecar_tasks.SdkDynamicSidecarTask)(
            task_function=fn,
            task_type=_common_constants.SdkTaskType.SIDECAR_TASK,
            discovery_version=cache_version,
            retries=retries,
            interruptible=interruptible,
            deprecated=deprecated,
            storage_request=storage_request,
            cpu_request=cpu_request,
            gpu_request=gpu_request,
            memory_request=memory_request,
            storage_limit=storage_limit,
            cpu_limit=cpu_limit,
            gpu_limit=gpu_limit,
            memory_limit=memory_limit,
            discoverable=cache,
            timeout=timeout or _datetime.timedelta(seconds=0),
            allowed_failure_ratio=allowed_failure_ratio,
            max_concurrency=max_concurrency,
            environment=environment,
            pod_spec=pod_spec,
            primary_container_name=primary_container_name,
        )

    if _task_function:
        return wrapper(_task_function)
    else:
        return wrapper


def pytorch_task(
    _task_function=None,
    cache_version="",
    retries=0,
    interruptible=False,
    deprecated="",
    cache=False,
    timeout=None,
    workers_count=1,
    per_replica_storage_request="",
    per_replica_cpu_request="",
    per_replica_gpu_request="",
    per_replica_memory_request="",
    per_replica_storage_limit="",
    per_replica_cpu_limit="",
    per_replica_gpu_limit="",
    per_replica_memory_limit="",
    environment=None,
    cls=None,
):
    """
    Decorator to create a Pytorch Task definition. This task will submit PyTorchJob (see https://github.com/kubeflow/pytorch-operator)
        defined by the code within the _task_function to k8s cluster.

    .. code-block:: python

        @inputs(int_list=[Types.Integer])
        @outputs(result=Types.Integer
        @pytorch_task(
            workers_count=2,
            per_replica_cpu_request="500m",
            per_replica_memory_request="4Gi",
            per_replica_memory_limit="8Gi",
            per_replica_gpu_limit="1",
        )
        def my_pytorch_job(wf_params, int_list, result):
            pass

    :param _task_function: this is the decorated method and shouldn't be declared explicitly.  The function must
        take a first argument, and then named arguments matching those defined in @inputs and @outputs.  No keyword
        arguments are allowed for wrapped task functions.

    :param Text cache_version: [optional] string representing logical version for discovery.  This field should be
        updated whenever the underlying algorithm changes.

        .. note::

            This argument is required to be a non-empty string if `cache` is True.

    :param int retries: [optional] integer determining number of times task can be retried on
        :py:exc:`flytekit.sdk.exceptions.RecoverableException` or transient platform failures.  Defaults
        to 0.

        .. note::

            If retries > 0, the task must be able to recover from any remote state created within the user code.  It is
            strongly recommended that tasks are written to be idempotent.

    :param bool interruptible: [optional] boolean describing if the task is interruptible.

    :param Text deprecated: [optional] string that should be provided if this task is deprecated.  The string
        will be logged as a warning so it should contain information regarding how to update to a newer task.

    :param bool cache: [optional] boolean describing if the outputs of this task should be cached and
        re-usable.

    :param datetime.timedelta timeout: [optional] describes how long the task should be allowed to
        run at max before triggering a retry (if retries are enabled).  By default, tasks are allowed to run
        indefinitely.  If a null timedelta is passed (i.e. timedelta(seconds=0)), the task will not timeout.

    :param int workers_count: integer determining the number of worker replicas spawned in the cluster for this job
        (in addition to 1 master).

    :param Text per_replica_storage_request: [optional] Kubernetes resource string for lower-bound of disk storage space
        for each replica spawned for this job (i.e. both for master and workers).  Default is set by platform-level configuration.

        .. note::

            This is currently not supported by the platform.

    :param Text per_replica_cpu_request: [optional] Kubernetes resource string for lower-bound of cores for each replica
        spawned for this job (i.e. both for master and workers).
        This can be set to a fractional portion of a CPU. Default is set by platform-level configuration.

        TODO: Add links to resource string documentation for Kubernetes

    :param Text per_replica_gpu_request: [optional] Kubernetes resource string for lower-bound of desired GPUs for each
        replica spawned for this job (i.e. both for master and workers).
        Default is set by platform-level configuration.

        TODO: Add links to resource string documentation for Kubernetes

    :param Text per_replica_memory_request: [optional]  Kubernetes resource string for lower-bound of physical memory
        necessary for each replica spawned for this job (i.e. both for master and workers).  Default is set by platform-level configuration.

        TODO: Add links to resource string documentation for Kubernetes

    :param Text per_replica_storage_limit: [optional] Kubernetes resource string for upper-bound of disk storage space
        for each replica spawned for this job (i.e. both for master and workers).
        This amount is not guaranteed!  If not specified, it is set equal to storage_request.

        .. note::

            This is currently not supported by the platform.

    :param Text per_replica_cpu_limit: [optional] Kubernetes resource string for upper-bound of cores for each replica
        spawned for this job (i.e. both for master and workers).
        This can be set to a fractional portion of a CPU. This amount is not guaranteed!  If not specified,
        it is set equal to cpu_request.

    :param Text per_replica_gpu_limit: [optional] Kubernetes resource string for upper-bound of desired GPUs for each
        replica spawned for this job (i.e. both for master and workers).
        This amount is not guaranteed!  If not specified, it is set equal to gpu_request.

    :param Text per_replica_memory_limit: [optional]  Kubernetes resource string for upper-bound of physical memory
        necessary for each replica spawned for this job (i.e. both for master and workers).
        This amount is not guaranteed!  If not specified, it is set equal to memory_request.

    :param dict[Text,Text] environment: [optional] environment variables to set when executing this task.

    :param cls: This can be used to override the task implementation with a user-defined extension. The class
        provided must be a subclass of flytekit.common.tasks.sdk_runnable.SdkRunnableTask.  A user can use this to
        inject bespoke logic into the base Flyte programming model.

    :rtype: flytekit.common.tasks.sdk_runnable.SdkRunnableTask
    """

    def wrapper(fn):
        return (cls or _sdk_pytorch_tasks.SdkPyTorchTask)(
            task_function=fn,
            task_type=_common_constants.SdkTaskType.PYTORCH_TASK,
            discovery_version=cache_version,
            retries=retries,
            interruptible=interruptible,
            deprecated=deprecated,
            discoverable=cache,
            timeout=timeout or _datetime.timedelta(seconds=0),
            workers_count=workers_count,
            per_replica_storage_request=per_replica_storage_request,
            per_replica_cpu_request=per_replica_cpu_request,
            per_replica_gpu_request=per_replica_gpu_request,
            per_replica_memory_request=per_replica_memory_request,
            per_replica_storage_limit=per_replica_storage_limit,
            per_replica_cpu_limit=per_replica_cpu_limit,
            per_replica_gpu_limit=per_replica_gpu_limit,
            per_replica_memory_limit=per_replica_memory_limit,
            environment=environment or {},
        )

    if _task_function:
        return wrapper(_task_function)
    else:
        return wrapper


def tensorflow_task(
    _task_function=None,
    cache_version="",
    retries=0,
    interruptible=False,
    deprecated="",
    cache=False,
    timeout=None,
    workers_count=1,
    ps_replicas_count=None,
    chief_replicas_count=None,
    per_replica_storage_request="",
    per_replica_cpu_request="",
    per_replica_gpu_request="",
    per_replica_memory_request="",
    per_replica_storage_limit="",
    per_replica_cpu_limit="",
    per_replica_gpu_limit="",
    per_replica_memory_limit="",
    environment=None,
    cls=None,
):
    """
    Decorator to create a Tensorflow Task definition. This task will submit TFJob (see https://github.com/kubeflow/tf-operator)
        defined by the code within the _task_function to k8s cluster.

    .. code-block:: python

        @inputs(int_list=[Types.Integer])
        @outputs(result=Types.Integer
        @tensorflow_task(
            workers_count=2,
            ps_replicas_count=1,
            chief_replicas_count=1,
            per_replica_cpu_request="500m",
            per_replica_memory_request="4Gi",
            per_replica_memory_limit="8Gi",
            per_replica_gpu_limit="1",
        )
        def my_tensorflow_job(wf_params, int_list, result):
            pass

    :param _task_function: this is the decorated method and shouldn't be declared explicitly.  The function must
        take a first argument, and then named arguments matching those defined in @inputs and @outputs.  No keyword
        arguments are allowed for wrapped task functions.

    :param Text cache_version: [optional] string representing logical version for discovery.  This field should be
        updated whenever the underlying algorithm changes.

        .. note::

            This argument is required to be a non-empty string if `cache` is True.

    :param int retries: [optional] integer determining number of times task can be retried on
        :py:exc:`flytekit.sdk.exceptions.RecoverableException` or transient platform failures.  Defaults
        to 0.

        .. note::

            If retries > 0, the task must be able to recover from any remote state created within the user code.  It is
            strongly recommended that tasks are written to be idempotent.

    :param bool interruptible: [optional] boolean describing if the task is interruptible.

    :param Text deprecated: [optional] string that should be provided if this task is deprecated.  The string
        will be logged as a warning so it should contain information regarding how to update to a newer task.

    :param bool cache: [optional] boolean describing if the outputs of this task should be cached and
        re-usable.

    :param datetime.timedelta timeout: [optional] describes how long the task should be allowed to
        run at max before triggering a retry (if retries are enabled).  By default, tasks are allowed to run
        indefinitely.  If a null timedelta is passed (i.e. timedelta(seconds=0)), the task will not timeout.

    :param int workers_count: integer determining the number of worker replicas spawned in the cluster for this job

    :param int ps_replicas_count: integer determining the number of parameter server replicas spawned in the cluster for this job

    :param int chief_replicas_count: integer determining the number of chief server replicas spawned in the cluster for this job

    :param Text per_replica_storage_request: [optional] Kubernetes resource string for lower-bound of disk storage space
        for each replica spawned for this job (i.e. both for parameter, chief server and workers).  Default is set by platform-level configuration.

        .. note::

            This is currently not supported by the platform.

    :param Text per_replica_cpu_request: [optional] Kubernetes resource string for lower-bound of cores for each replica
        spawned for this job (i.e. both for parameter, chief server and workers).
        This can be set to a fractional portion of a CPU. Default is set by platform-level configuration.

        TODO: Add links to resource string documentation for Kubernetes

    :param Text per_replica_gpu_request: [optional] Kubernetes resource string for lower-bound of desired GPUs for each
        replica spawned for this job (i.e. both for parameter, chief server and workers).
        Default is set by platform-level configuration.

        TODO: Add links to resource string documentation for Kubernetes

    :param Text per_replica_memory_request: [optional]  Kubernetes resource string for lower-bound of physical memory
        necessary for each replica spawned for this job (i.e. both for parameter, chief server and workers).  Default is set by platform-level configuration.

        TODO: Add links to resource string documentation for Kubernetes

    :param Text per_replica_storage_limit: [optional] Kubernetes resource string for upper-bound of disk storage space
        for each replica spawned for this job (i.e. both for parameter, chief server and workers).
        This amount is not guaranteed!  If not specified, it is set equal to storage_request.

        .. note::

            This is currently not supported by the platform.

    :param Text per_replica_cpu_limit: [optional] Kubernetes resource string for upper-bound of cores for each replica
        spawned for this job (i.e. both for parameter, chief server and workers).
        This can be set to a fractional portion of a CPU. This amount is not guaranteed!  If not specified,
        it is set equal to cpu_request.

    :param Text per_replica_gpu_limit: [optional] Kubernetes resource string for upper-bound of desired GPUs for each
        replica spawned for this job (i.e. both for parameter, chief server and workers).
        This amount is not guaranteed!  If not specified, it is set equal to gpu_request.

    :param Text per_replica_memory_limit: [optional]  Kubernetes resource string for upper-bound of physical memory
        necessary for each replica spawned for this job (i.e. both for parameter, chief server and workers).
        This amount is not guaranteed!  If not specified, it is set equal to memory_request.

    :param dict[Text,Text] environment: [optional] environment variables to set when executing this task.

    :param cls: This can be used to override the task implementation with a user-defined extension. The class
        provided must be a subclass of flytekit.common.tasks.sdk_runnable.SdkRunnableTask.  A user can use this to
        inject bespoke logic into the base Flyte programming model.

    :rtype: flytekit.common.tasks.sdk_runnable.SdkRunnableTask
    """

    def wrapper(fn):
        return (cls or _sdk_tensorflow_tasks.SdkTensorFlowTask)(
            task_function=fn,
            task_type=_common_constants.SdkTaskType.TENSORFLOW_TASK,
            cache_version=cache_version,
            retries=retries,
            interruptible=interruptible,
            deprecated=deprecated,
            cache=cache,
            timeout=timeout or _datetime.timedelta(seconds=0),
            workers_count=workers_count,
            ps_replicas_count=ps_replicas_count,
            chief_replicas_count=chief_replicas_count,
            per_replica_storage_request=per_replica_storage_request,
            per_replica_cpu_request=per_replica_cpu_request,
            per_replica_gpu_request=per_replica_gpu_request,
            per_replica_memory_request=per_replica_memory_request,
            per_replica_storage_limit=per_replica_storage_limit,
            per_replica_cpu_limit=per_replica_cpu_limit,
            per_replica_gpu_limit=per_replica_gpu_limit,
            per_replica_memory_limit=per_replica_memory_limit,
            environment=environment or {},
        )

    if _task_function:
        return wrapper(_task_function)
    else:
        return wrapper
