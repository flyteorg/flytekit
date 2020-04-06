from __future__ import absolute_import

import os as _os

import itertools as _itertools
import math
import six as _six

from flytekit.common import constants as _constants, interface as _interface, sdk_bases as _sdk_bases, \
    launch_plan as _launch_plan, workflow as _workflow
from flytekit.common.core import identifier as _identifier
from flytekit.common.exceptions import scopes as _exception_scopes
from flytekit.common.tasks import output as _task_output, sdk_runnable as _sdk_runnable
from flytekit.common.types import helpers as _type_helpers
from flytekit.common.utils import _dnsify
from flytekit.models import literals as _literal_models, dynamic_job as _dynamic_job, array_job as _array_job
from flytekit.models.core import identifier as _identifier_model
from flytekit.common.mixins import registerable as _registerable
from flytekit.configuration import platform as _platform_config, internal as _internal_config


class PromiseOutputReference(_task_output.OutputReference):
    @property
    def raw_value(self):
        """
        :rtype: T
        """
        return self._raw_value

    @_exception_scopes.system_entry_point
    def set(self, value):
        """
        This should be called to set the value for output.  The SDK will apply the appropriate type and value checking.
        It will raise an exception if necessary.
        :param T value:
        :raises: flytekit.common.exceptions.user.FlyteValueException
        """

        self._raw_value = value


class SdkDynamicTask(_six.with_metaclass(_sdk_bases.ExtendedSdkType, _sdk_runnable.SdkRunnableTask)):
    """
    This class includes the additional logic for building a task that executes parent-child tasks in Python code.  It
    has even more validation checks to ensure proper behavior than it's superclasses.

    Since an SdkDynamicTask is assumed to run by hooking into Python code, we will provide additional shortcuts and
    methods on this object.
    """

    def __init__(
            self,
            task_function,
            task_type,
            discovery_version,
            retries,
            interruptible,
            deprecated,
            storage_request,
            cpu_request,
            gpu_request,
            memory_request,
            storage_limit,
            cpu_limit,
            gpu_limit,
            memory_limit,
            discoverable,
            timeout,
            allowed_failure_ratio,
            max_concurrency,
            environment,
            custom
    ):
        """
        :param task_function: Function container user code.  This will be executed via the SDK's engine.
        :param Text task_type: string describing the task type
        :param Text discovery_version: string describing the version for task discovery purposes
        :param int retries: Number of retries to attempt
        :param bool interruptible: Whether or not task is interruptible
        :param Text deprecated:
        :param Text storage_request:
        :param Text cpu_request:
        :param Text gpu_request:
        :param Text memory_request:
        :param Text storage_limit:
        :param Text cpu_limit:
        :param Text gpu_limit:
        :param Text memory_limit:
        :param bool discoverable:
        :param datetime.timedelta timeout:
        :param float allowed_failure_ratio:
        :param int max_concurrency:
        :param dict[Text, Text] environment:
        :param dict[Text, T] custom:
        """
        super(SdkDynamicTask, self).__init__(
            task_function, task_type, discovery_version, retries, interruptible, deprecated,
            storage_request, cpu_request, gpu_request, memory_request, storage_limit,
            cpu_limit, gpu_limit, memory_limit, discoverable, timeout, environment, custom)

        # These will only appear in the generated futures
        self._allowed_failure_ratio = allowed_failure_ratio
        self._max_concurrency = max_concurrency

    def _create_array_job(self, inputs_prefix):
        """
        Creates an array job for the passed sdk_task.
        :param str inputs_prefix:
        :rtype: _array_job.ArrayJob
        """
        return _array_job.ArrayJob(parallelism=self._max_concurrency if self._max_concurrency else 0,
                                   size=1,
                                   min_successes=1)

    @staticmethod
    def _can_run_as_array(task_type):
        """
        Checks if a task can be grouped to run as an array job.
        :param Text task_type:
        :rtype: bool
        """
        return task_type == _constants.SdkTaskType.PYTHON_TASK

    def _produce_dynamic_job_spec(self, context, inputs):
        """
        Runs user code and and produces future task nodes to run sub-tasks.
        :param context:
        :param flytekit.models.literals.LiteralMap literal_map inputs:
        :rtype: (_dynamic_job.DynamicJobSpec, dict[Text, flytekit.models.common.FlyteIdlEntity])
        """
        inputs_dict = _type_helpers.unpack_literal_map_to_sdk_python_std(inputs, {
            k: _type_helpers.get_sdk_type_from_literal_type(v.type) for k, v in _six.iteritems(self.interface.inputs)
        })
        outputs_dict = {
            name: PromiseOutputReference(_type_helpers.get_sdk_type_from_literal_type(variable.type))
            for name, variable in _six.iteritems(self.interface.outputs)
        }

        # Because users declare both inputs and outputs in their functions signatures, merge them together
        # before calling user code
        inputs_dict.update(outputs_dict)
        yielded_sub_tasks = [sub_task for sub_task in
                             super(SdkDynamicTask, self)._execute_user_code(context, inputs_dict) or []]

        upstream_nodes = list()
        output_bindings = [_literal_models.Binding(var=name, binding=_interface.BindingData.from_python_std(
            b.sdk_type.to_flyte_literal_type(), b.raw_value, upstream_nodes=upstream_nodes))
                           for name, b in _six.iteritems(outputs_dict)]
        upstream_nodes = set(upstream_nodes)

        generated_files = {}
        # Keeping future-tasks in original order. We don't use upstream_nodes exclusively because the parent task can
        # yield sub-tasks that it never uses to produce final outputs but they need to execute nevertheless.
        array_job_index = {}
        tasks = []
        nodes = []
        visited_nodes = set()
        generated_ids = {}
        effective_failure_ratio = self._allowed_failure_ratio or 0.0

        # TODO: This function needs to be cleaned up.
        # The reason we chain these two together is because we allow users to not have to explicitly "yield" the
        # node. As long as the subtask/lp/subwf has an output that's referenced, it'll get picked up.
        for sub_task_node in _itertools.chain(yielded_sub_tasks, upstream_nodes):
            if sub_task_node in visited_nodes:
                continue
            visited_nodes.add(sub_task_node)
            executable = sub_task_node.executable_sdk_object

            # If the executable object that we're dealing with is registerable (ie, SdkRunnableLaunchPlan, SdkWorkflow
            # SdkTask, or SdkRunnableTask), then it should have the ability to give itself a name. After assigning
            # itself the name, also make sure the id is properly set according to current config values.
            if isinstance(executable, _registerable.RegisterableEntity):
                executable.auto_assign_name()
                executable._id = _identifier.Identifier(
                    executable.resource_type,
                    _internal_config.TASK_PROJECT.get() or _internal_config.PROJECT.get(),
                    _internal_config.TASK_DOMAIN.get() or _internal_config.DOMAIN.get(),
                    executable.platform_valid_name,
                    _internal_config.TASK_VERSION.get() or _internal_config.VERSION.get()
                )

            # Generate an id that's unique in the document (if the same task is used multiple times with
            # different resources, executable_sdk_object.id will be the same but generated node_ids should not
            # be.
            safe_task_id = _six.text_type(sub_task_node.executable_sdk_object.id)
            if safe_task_id in generated_ids:
                new_count = generated_ids[safe_task_id] = generated_ids[safe_task_id] + 1
            else:
                new_count = generated_ids[safe_task_id] = 0
            unique_node_id = _dnsify("{}-{}".format(safe_task_id, new_count))

            # Handling cases where the yielded nodes are launch plan or subworkflow nodes
            if isinstance(sub_task_node.executable_sdk_object, (_launch_plan.SdkLaunchPlan, _workflow.SdkWorkflow)):
                node = sub_task_node.assign_id_and_return(unique_node_id)
                nodes.append(node)
                for k, node_output in _six.iteritems(sub_task_node.outputs):
                    if not node_output.sdk_node.id:
                        node_output.sdk_node.assign_id_and_return(node.id)

                    # Upload inputs to working directory under /array_job.input_ref/inputs.pb
                    input_path = _os.path.join(node.id, _constants.INPUT_FILE_NAME)
                    generated_files[input_path] = _literal_models.LiteralMap(
                        literals={binding.var: binding.binding.to_literal_model() for binding in
                                  sub_task_node.inputs})

            # Handling tasks
            else:
                # If the task can run as an array job, group its instances together. Otherwise, keep each
                # invocation as a separate node.
                if SdkDynamicTask._can_run_as_array(sub_task_node.executable_sdk_object.type):
                    if sub_task_node.executable_sdk_object in array_job_index:
                        array_job, node = array_job_index[sub_task_node.executable_sdk_object]
                        array_job.size += 1
                        array_job.min_successes = int(math.ceil((1 - effective_failure_ratio) * array_job.size))
                    else:
                        array_job = self._create_array_job(inputs_prefix=unique_node_id)
                        node = sub_task_node.assign_id_and_return(unique_node_id)
                        array_job_index[sub_task_node.executable_sdk_object] = (array_job, node)

                    node_index = _six.text_type(array_job.size - 1)
                    for k, node_output in _six.iteritems(sub_task_node.outputs):
                        if not node_output.sdk_node.id:
                            node_output.sdk_node.assign_id_and_return(node.id)
                        node_output.var = "[{}].{}".format(node_index, node_output.var)

                    # Upload inputs to working directory under /array_job.input_ref/<index>/inputs.pb
                    input_path = _os.path.join(node.id, node_index, _constants.INPUT_FILE_NAME)
                    generated_files[input_path] = _literal_models.LiteralMap(
                        literals={binding.var: binding.binding.to_literal_model() for binding in
                                  sub_task_node.inputs})
                else:
                    node = sub_task_node.assign_id_and_return(unique_node_id)

                    tasks.append(sub_task_node.executable_sdk_object)
                    nodes.append(node)

                    for k, node_output in _six.iteritems(sub_task_node.outputs):
                        if not node_output.sdk_node.id:
                            node_output.sdk_node.assign_id_and_return(node.id)

                    # Upload inputs to working directory under /array_job.input_ref/inputs.pb
                    input_path = _os.path.join(node.id, _constants.INPUT_FILE_NAME)
                    generated_files[input_path] = _literal_models.LiteralMap(
                        literals={binding.var: binding.binding.to_literal_model() for binding in
                                  sub_task_node.inputs})

        # assign custom field to the ArrayJob properties computed.
        for task, (array_job, _) in _six.iteritems(array_job_index):
            # TODO: Reconstruct task template object instead of modifying an existing one?
            tasks.append(task.assign_custom_and_return(array_job.to_dict()).assign_type_and_return(
                _constants.SdkTaskType.CONTAINER_ARRAY_TASK))

        # min_successes is absolute, it's computed as the reverse of allowed_failure_ratio and multiplied by the
        # total length of tasks to get an absolute count.
        nodes.extend([array_job_node for (_, array_job_node) in array_job_index.values()])
        dynamic_job_spec = _dynamic_job.DynamicJobSpec(
            min_successes=len(nodes),
            tasks=tasks,
            nodes=nodes,
            outputs=output_bindings,
            subworkflows=[])

        return dynamic_job_spec, generated_files

    @_exception_scopes.system_entry_point
    def execute(self, context, inputs):
        """
        Executes batch task's user code and produces futures file as well as all sub-task inputs.pb files.

        :param flytekit.engines.common.EngineContext context:
        :param flytekit.models.literals.LiteralMap inputs:
        :rtype: dict[Text, flytekit.models.common.FlyteIdlEntity]
        :returns: This function must return a dictionary mapping 'filenames' to Flyte Interface Entities.  These
            entities will be used by the engine to pass data from node to node, populate metadata, etc. etc..  Each
            engine will have different behavior.  For instance, the Flyte engine will upload the entities to a remote
            working directory (with the names provided), which will in turn allow Flyte Propeller to push along the
            workflow.  Where as local engine will merely feed the outputs directly into the next node.
        """
        spec, generated_files = self._produce_dynamic_job_spec(context, inputs)

        # If no sub-tasks are requested to run, just produce an outputs file like any other single-step tasks.
        if len(generated_files) == 0:
            return {
                _constants.OUTPUT_FILE_NAME: _literal_models.LiteralMap(
                    literals={binding.var: binding.binding.to_literal_model() for binding in spec.outputs})
            }
        else:
            generated_files.update({
                _constants.FUTURES_FILE_NAME: spec
            })

            return generated_files
