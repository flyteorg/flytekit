import copy as _copy
import uuid as _uuid
from inspect import getfullargspec as _getargspec
from typing import List, Dict, Any

import six as _six
from six.moves import queue as _queue

import flytekit.legacy
import flytekit.platform

from flytekit.common import interface as _interface, promise as _promise, sdk_bases as _sdk_bases, \
    constants as _constants
from flytekit.common.core import identifier as _identifier
from flytekit.common.core.identifier import WorkflowExecutionIdentifier
from flytekit.common.exceptions import user as _user_exceptions, scopes as _exception_scopes
from flytekit.common.mixins import hash as _hash_mixin
from flytekit.common.tasks import output as _task_output
from flytekit.common.tasks.sdk_runnable import SdkRunnableTaskStyle, ExecutionParameters, SdkRunnableContainer
from flytekit.common.types import helpers as _type_helpers
from flytekit.configuration import internal as _internal_config, resources as _resource_config
from flytekit.legacy.engines import loader as _engine_loader
from flytekit.models import common as _common_models, launch_plan as _launch_plan_models, schedule as _schedule_model, \
    interface as _interface_models, literals as _literal_models, schedule as _schedule_models, types as _type_models, \
    task as _task_models
from flytekit.models.core import identifier as _identifier_model, workflow as _workflow_models
from flytekit.platform.sdk_launch_plan import SdkLaunchPlan


# The difference between this and the SdkLaunchPlan class is that this runnable class is supposed to only be used for
# launch plans loaded alongside the current Python interpreter.
from flytekit.platform.sdk_workflow import SdkWorkflow


class SdkRunnableLaunchPlan(_hash_mixin.HashOnReferenceMixin, SdkLaunchPlan):
    def __init__(
        self,
        sdk_workflow,
        default_inputs=None,
        fixed_inputs=None,
        role=None,
        schedule=None,
        notifications=None,
        labels=None,
        annotations=None,
        auth_role=None,
        raw_output_data_config=None,
    ):
        """
        :param flytekit.legacy.runnables.SdkRunnableWorkflow sdk_workflow:
        :param dict[Text,flytekit.common.promise.Input] default_inputs:
        :param dict[Text,Any] fixed_inputs: These inputs will be fixed and not need to be set when executing this
            launch plan.
        :param Text role: Deprecated. IAM role to execute this launch plan with.
        :param flytekit.models.schedule.Schedule: Schedule to apply to this workflow.
        :param list[flytekit.models.common.Notification]: List of notifications to apply to this launch plan.
        :param flytekit.models.common.Labels labels: Any custom kubernetes labels to apply to workflows executed by this
            launch plan.
        :param flytekit.models.common.Annotations annotations: Any custom kubernetes annotations to apply to workflows
            executed by this launch plan.
            Any custom kubernetes annotations to apply to workflows executed by this launch plan.
        :param flytekit.models.common.Authrole auth_role: The auth method with which to execute the workflow.
        :param flytekit.models.common.RawOutputDataConfig raw_output_data_config: Config for offloading data
        """
        if role and auth_role:
            raise ValueError("Cannot set both role and auth. Role is deprecated, use auth instead.")

        fixed_inputs = fixed_inputs or {}
        default_inputs = default_inputs or {}

        if role:
            auth_role = _common_models.AuthRole(assumable_iam_role=role)

        # The constructor for SdkLaunchPlan sets the id to None anyways so we don't bother passing in an ID. The ID
        # should be set in one of three places,
        #   1) When the object is registered (in the code above)
        #   2) By the dynamic task code after this runnable object has already been __call__'ed. The SdkNode produced
        #      maintains a link to this object and will set the ID according to the configuration variables present.
        #   3) When SdkLaunchPlan.fetch() is run
        super(SdkRunnableLaunchPlan, self).__init__(
            None,
            _launch_plan_models.LaunchPlanMetadata(
                schedule=schedule or _schedule_model.Schedule(""), notifications=notifications or [],
            ),
            _interface_models.ParameterMap(default_inputs),
            _type_helpers.pack_python_std_map_to_literal_map(
                fixed_inputs,
                {
                    k: _type_helpers.get_sdk_type_from_literal_type(var.type)
                    for k, var in _six.iteritems(sdk_workflow.interface.inputs)
                    if k in fixed_inputs
                },
            ),
            labels or _common_models.Labels({}),
            annotations or _common_models.Annotations({}),
            auth_role,
            raw_output_data_config or _common_models.RawOutputDataConfig(""),
        )
        self._interface = _interface.TypedInterface(
            {k: v.var for k, v in _six.iteritems(default_inputs)}, sdk_workflow.interface.outputs,
        )
        self._upstream_entities = {sdk_workflow}
        self._sdk_workflow = sdk_workflow

    @classmethod
    def from_flyte_idl(cls, _):
        raise _user_exceptions.FlyteAssertion(
            "An SdkRunnableLaunchPlan must be created from a reference to local Python code only."
        )

    @classmethod
    def promote_from_model(cls, model):
        raise _user_exceptions.FlyteAssertion(
            "An SdkRunnableLaunchPlan must be created from a reference to local Python code only."
        )

    @classmethod
    @_exception_scopes.system_entry_point
    def fetch(cls, project, domain, name, version=None):
        """
        This function uses the engine loader to call create a hydrated task from Admin.
        :param Text project:
        :param Text domain:
        :param Text name:
        :param Text version:
        :rtype: SdkRunnableLaunchPlan
        """
        raise _user_exceptions.FlyteAssertion(
            "An SdkRunnableLaunchPlan must be created from a reference to local Python code only."
        )

    @property
    def workflow_id(self):
        """
        :rtype: flytekit.common.core.identifier.Identifier
        """
        return self._sdk_workflow.id

    def __repr__(self):
        """
        :rtype: Text
        """
        return "SdkRunnableLaunchPlan(ID: {} Interface: {} WF ID: {})".format(self.id, self.interface, self.workflow_id)


class SdkRunnableWorkflow(SdkWorkflow):
    """
    Wrapper class for workflows defined using Python, written in a Flyte workflow repo. This class is misnamed. It is
    more appropriately called PythonWorkflow. The reason we are calling it SdkRunnableWorkflow instead is merely in
    keeping with the established convention in other parts of this codebase. We will likely change this naming scheme
    entirely before a 1.0 release.

    Being "runnable" or not "runnable" is not a distinction we care to make at this point. Do not read into it,
    pretend it's not there. The purpose of this class is merely to differentiate between
      i) A workflow object, as created by a user's workflow code, using the @workflow_class decorator for instance. If
         you have one of these classes, it means you have the actual Python code available in the Python process you
         are running.
      ii) The SdkWorkflow object, which represents a workflow as retrieved from Flyte Admin. Anyone with access
          to Admin, can instantiate an SdkWorkflow object by 'fetch'ing it. You don't need to have any of
          the actual code checked out. SdkWorkflow's are effectively then the control plane model of a workflow,
          represented as a Python object.
    """

    def __init__(
        self,
        inputs: List[Input],
        nodes: List[flytekit.platform.sdk_node.SdkNode],
        interface,
        output_bindings,
        id=None,
        metadata=None,
        metadata_defaults=None,
        disable_default_launch_plan=False,
    ):
        """
        :param list[flytekit.common.nodes.SdkNode] nodes:
        :param flytekit.models.interface.TypedInterface interface: Defines a strongly typed interface for the
            Workflow (inputs, outputs).  This can include some optional parameters.
        :param list[flytekit.models.literals.Binding] output_bindings: A list of output bindings that specify how to construct
            workflow outputs. Bindings can pull node outputs or specify literals. All workflow outputs specified in
            the interface field must be bound
            in order for the workflow to be validated. A workflow has an implicit dependency on all of its nodes
            to execute successfully in order to bind final outputs.
        :param flytekit.models.core.identifier.Identifier id: This is an autogenerated id by the system. The id is
            globally unique across Flyte.
        :param WorkflowMetadata metadata: This contains information on how to run the workflow.
        :param flytekit.models.core.workflow.WorkflowMetadataDefaults metadata_defaults: Defaults to be passed
            to nodes contained within workflow.
        :param bool disable_default_launch_plan: Determines whether to create a default launch plan for the workflow.
        """
        # Save the promise.Input objects for future use.
        self._user_inputs = inputs

        # Set optional settings
        id = (
            id
            if id is not None
            else _identifier.Identifier(
                _identifier_model.ResourceType.WORKFLOW,
                _internal_config.PROJECT.get(),
                _internal_config.DOMAIN.get(),
                _uuid.uuid4().hex,
                _internal_config.VERSION.get(),
            )
        )
        metadata = metadata if metadata is not None else _workflow_models.WorkflowMetadata()
        metadata_defaults = (
            metadata_defaults if metadata_defaults is not None else _workflow_models.WorkflowMetadataDefaults()
        )

        super(SdkRunnableWorkflow, self).__init__(
            nodes=nodes,
            interface=interface,
            output_bindings=output_bindings,
            id=id,
            metadata=metadata,
            metadata_defaults=metadata_defaults,
        )

        # Set this last as it's set in constructor
        self._upstream_entities = set(n.executable_sdk_object for n in nodes)
        self._should_create_default_launch_plan = not disable_default_launch_plan

    @property
    def should_create_default_launch_plan(self):
        """
        Determines whether registration flow should create a default launch plan for this workflow or not.
        :rtype: bool
        """
        return self._should_create_default_launch_plan

    def __call__(self, *args, **input_map):
        # Take the default values from the Inputs
        compiled_inputs = {v.name: v.sdk_default for v in self.user_inputs if not v.sdk_required}
        compiled_inputs.update(input_map)

        return super().__call__(*args, **compiled_inputs)

    @classmethod
    def construct_from_class_definition(
        cls,
        inputs: List[Input],
        outputs: List[Output],
        nodes: List[flytekit.platform.sdk_node.SdkNode],
        metadata: _workflow_models.WorkflowMetadata = None,
        metadata_defaults: _workflow_models.WorkflowMetadataDefaults = None,
        disable_default_launch_plan: bool = False,
    ) -> "SdkRunnableWorkflow":
        """
        This constructor is here to provide backwards-compatibility for class-defined Workflows

        :param list[flytekit.common.promise.Input] inputs:
        :param list[Output] outputs:
        :param list[flytekit.common.nodes.SdkNode] nodes:
        :param WorkflowMetadata metadata: This contains information on how to run the workflow.
        :param flytekit.models.core.workflow.WorkflowMetadataDefaults metadata_defaults: Defaults to be passed
            to nodes contained within workflow.
        :param bool disable_default_launch_plan: Determines whether to create a default launch plan for the workflow or not.

        :rtype: SdkRunnableWorkflow
        """
        for n in nodes:
            for upstream in n.upstream_nodes:
                if upstream.id is None:
                    raise _user_exceptions.FlyteAssertion(
                        "Some nodes contained in the workflow were not found in the workflow description.  Please "
                        "ensure all nodes are either assigned to attributes within the class or an element in a "
                        "list, dict, or tuple which is stored as an attribute in the class."
                    )

        id = _identifier.Identifier(
            _identifier_model.ResourceType.WORKFLOW,
            _internal_config.PROJECT.get(),
            _internal_config.DOMAIN.get(),
            _uuid.uuid4().hex,
            _internal_config.VERSION.get(),
        )
        interface = _interface.TypedInterface({v.name: v.var for v in inputs}, {v.name: v.var for v in outputs})

        output_bindings = [_literal_models.Binding(v.name, v.binding_data) for v in outputs]

        return cls(
            inputs=inputs,
            nodes=nodes,
            interface=interface,
            output_bindings=output_bindings,
            id=id,
            metadata=metadata,
            metadata_defaults=metadata_defaults,
            disable_default_launch_plan=disable_default_launch_plan,
        )

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, new_id):
        self._id = new_id

    @property
    def user_inputs(self) -> List[Input]:
        """
        :rtype: list[flytekit.common.promise.Input]
        """
        return self._user_inputs

    def create_launch_plan(
        self,
        default_inputs: Dict[str, Input] = None,
        fixed_inputs: Dict[str, Any] = None,
        schedule: _schedule_models.Schedule = None,
        role: str = None,
        notifications: List[_common_models.Notification] = None,
        labels: _common_models.Labels = None,
        annotations: _common_models.Annotations = None,
        assumable_iam_role: str = None,
        kubernetes_service_account: str = None,
        raw_output_data_prefix: str = None,
    ):
        """
        This method will create a launch plan object that can execute this workflow.
        :param dict[Text,flytekit.common.promise.Input] default_inputs:
        :param dict[Text,T] fixed_inputs:
        :param flytekit.models.schedule.Schedule schedule: A schedule on which to execute this launch plan.
        :param Text role: Deprecated. Use assumable_iam_role instead.
        :param list[flytekit.models.common.Notification] notifications: A list of notifications to enact by default for
        this launch plan.
        :param flytekit.models.common.Labels labels:
        :param flytekit.models.common.Annotations annotations:
        :param cls: This parameter can be used by users to define an extension of a launch plan to instantiate.  The
        class provided should be a subclass of flytekit.common.launch_plan.SdkLaunchPlan.
        :param Text assumable_iam_role: The IAM role to execute the workflow with.
        :param Text kubernetes_service_account: The kubernetes service account to execute the workflow with.
        :param Text raw_output_data_prefix: Bucket for offloaded data
        :rtype: flytekit.legacy.runnables.SdkRunnableLaunchPlan
        """
        # TODO: Actually ensure the parameters conform.
        if role and (assumable_iam_role or kubernetes_service_account):
            raise ValueError("Cannot set both role and auth. Role is deprecated, use auth instead.")
        fixed_inputs = fixed_inputs or {}
        merged_default_inputs = {v.name: v for v in self.user_inputs if v.name not in fixed_inputs}
        merged_default_inputs.update(default_inputs or {})

        if role:
            assumable_iam_role = role  # For backwards compatibility
        auth_role = _common_models.AuthRole(
            assumable_iam_role=assumable_iam_role, kubernetes_service_account=kubernetes_service_account,
        )

        raw_output_config = _common_models.RawOutputDataConfig(raw_output_data_prefix or "")

        return flytekit.legacy.runnables.SdkRunnableLaunchPlan(
            sdk_workflow=self,
            default_inputs={
                k: user_input.rename_and_return_reference(k) for k, user_input in _six.iteritems(merged_default_inputs)
            },
            fixed_inputs=fixed_inputs,
            schedule=schedule,
            notifications=notifications,
            labels=labels,
            annotations=annotations,
            auth_role=auth_role,
            raw_output_data_config=raw_output_config,
        )


class Output(object):
    def __init__(self, name, value, sdk_type=None, help=None):
        """
        :param Text name:
        :param T value:
        :param U sdk_type: If specified, the value provided must cast to this type.  Normally should be an instance of
            flytekit.common.types.base_sdk_types.FlyteSdkType.  But could also be something like:

            list[flytekit.common.types.base_sdk_types.FlyteSdkType],
            dict[flytekit.common.types.base_sdk_types.FlyteSdkType,flytekit.common.types.base_sdk_types.FlyteSdkType],
            (flytekit.common.types.base_sdk_types.FlyteSdkType, flytekit.common.types.base_sdk_types.FlyteSdkType, ...)
        """
        if sdk_type is None:
            # This syntax didn't work for some reason: sdk_type = sdk_type or Output._infer_type(value)
            sdk_type = Output._infer_type(value)
        sdk_type = _type_helpers.python_std_to_sdk_type(sdk_type)

        self._binding_data = _interface.BindingData.from_python_std(sdk_type.to_flyte_literal_type(), value)
        self._var = _interface_models.Variable(sdk_type.to_flyte_literal_type(), help or "")
        self._name = name

    def rename_and_return_reference(self, new_name):
        self._name = new_name
        return self

    @staticmethod
    def _infer_type(value):
        # TODO: Infer types
        raise NotImplementedError(
            "Currently the SDK cannot infer a workflow output type, so please use the type kwarg "
            "when instantiating an output."
        )

    @property
    def name(self):
        """
        :rtype: Text
        """
        return self._name

    @property
    def binding_data(self):
        """
        :rtype: flytekit.models.literals.BindingData
        """
        return self._binding_data

    @property
    def var(self):
        """
        :rtype: flytekit.models.interface.Variable
        """
        return self._var


def build_sdk_workflow_from_metaclass(metaclass, on_failure=None, disable_default_launch_plan=False, cls=None):
    """
    :param T metaclass: This is the user-defined workflow class, prior to decoration.
    :param on_failure flytekit.models.core.workflow.WorkflowMetadata.OnFailurePolicy: [Optional] The execution policy
    when the workflow detects a failure.
    :param bool disable_default_launch_plan: Determines whether to create a default launch plan for the workflow or not.
    :param cls: This is the class that will be instantiated from the inputs, outputs, and nodes.  This will be used
        by users extending the base Flyte programming model. If set, it must be a subclass of PythonWorkflow.

    :rtype: flytekit.legacy.runnables.SdkRunnableWorkflow
    """
    inputs, outputs, nodes = _discover_workflow_components(metaclass)
    metadata = _workflow_models.WorkflowMetadata(on_failure=on_failure if on_failure else None)

    return (cls or SdkRunnableWorkflow).construct_from_class_definition(
        inputs=[i for i in sorted(inputs, key=lambda x: x.name)],
        outputs=[o for o in sorted(outputs, key=lambda x: x.name)],
        nodes=[n for n in sorted(nodes, key=lambda x: x.id)],
        metadata=metadata,
        disable_default_launch_plan=disable_default_launch_plan,
    )


def _discover_workflow_components(workflow_class):
    """
    This task iterates over the attributes of a user-defined class in order to return a list of inputs, outputs and
    nodes.
    :param class workflow_class: User-defined class with task instances as attributes.
    :rtype: (list[flytekit.common.promise.Input], list[flytekit.legacy.runnables.Output], list[flytekit.common.nodes.SdkNode])
    """

    inputs = []
    outputs = []
    nodes = []

    to_visit_objs = _queue.Queue()
    top_level_attributes = set()
    for attribute_name in dir(workflow_class):
        to_visit_objs.put((attribute_name, getattr(workflow_class, attribute_name)))
        top_level_attributes.add(attribute_name)

    # For all task instances defined within the workflow, bind them to this specific workflow and hook-up to the
    # engine (when available)
    visited_obj_ids = set()
    while not to_visit_objs.empty():
        attribute_name, current_obj = to_visit_objs.get()

        current_obj_id = id(current_obj)
        if current_obj_id in visited_obj_ids:
            continue
        visited_obj_ids.add(current_obj_id)

        if isinstance(current_obj, flytekit.platform.sdk_node.SdkNode):
            # TODO: If an attribute name is on the form node_name[index], the resulting
            # node name might not be correct.
            nodes.append(current_obj.assign_id_and_return(attribute_name))
        elif isinstance(current_obj, Input):
            if attribute_name is None or attribute_name not in top_level_attributes:
                raise _user_exceptions.FlyteValueException(
                    attribute_name, "Detected workflow input specified outside of top level.",
                )
            inputs.append(current_obj.rename_and_return_reference(attribute_name))
        elif isinstance(current_obj, Output):
            if attribute_name is None or attribute_name not in top_level_attributes:
                raise _user_exceptions.FlyteValueException(
                    attribute_name, "Detected workflow output specified outside of top level.",
                )
            outputs.append(current_obj.rename_and_return_reference(attribute_name))
        elif isinstance(current_obj, list) or isinstance(current_obj, set) or isinstance(current_obj, tuple):
            for idx, value in enumerate(current_obj):
                to_visit_objs.put((_assign_indexed_attribute_name(attribute_name, idx), value))
        elif isinstance(current_obj, dict):
            # Visit dictionary keys.
            for key in current_obj.keys():
                to_visit_objs.put((_assign_indexed_attribute_name(attribute_name, key), key))
            # Visit dictionary values.
            for key, value in _six.iteritems(current_obj):
                to_visit_objs.put((_assign_indexed_attribute_name(attribute_name, key), value))
    return inputs, outputs, nodes


def _assign_indexed_attribute_name(attribute_name, index):
    return "{}[{}]".format(attribute_name, index)


class Input(_interface_models.Parameter, metaclass=_sdk_bases.ExtendedSdkType):
    def __init__(self, name, sdk_type, help=None, **kwargs):
        """
        :param Text name:
        :param flytekit.common.types.base_sdk_types.FlyteSdkType sdk_type: This is the SDK type necessary to create an
            input to this workflow.
        :param Text help: An optional help string to describe the input to users.
        :param bool required: If set to True, default must be None
        :param T default:  If this is not a required input, the value will default to this value.
        """
        param_default = None
        if "required" not in kwargs and "default" not in kwargs:
            # Neither required or default is set so assume required
            required = True
            default = None
        elif kwargs.get("required", False) and "default" in kwargs:
            # Required cannot be set to True and have a default specified
            raise _user_exceptions.FlyteAssertion("Default cannot be set when required is True")
        elif "default" in kwargs:
            # If default is specified, then required must be false and the value is whatever is specified
            required = None
            default = kwargs["default"]
            param_default = sdk_type.from_python_std(default)
        else:
            # If no default is set, but required is set, then the behavior is determined by required == True or False
            default = None
            required = kwargs["required"]
            if not required:
                # If required == False, we assume default to be None
                param_default = sdk_type.from_python_std(default)
                required = None

        self._sdk_required = required or False
        self._sdk_default = default
        self._help = help
        self._sdk_type = sdk_type
        self._promise = _type_models.OutputReference(_constants.GLOBAL_INPUT_NODE_ID, name)
        self._name = name
        super(Input, self).__init__(
            _interface_models.Variable(type=sdk_type.to_flyte_literal_type(), description=help or ""),
            required=required,
            default=param_default,
        )

    def rename_and_return_reference(self, new_name):
        self._promise._var = new_name
        return self

    @property
    def name(self):
        """
        :rtype: Text
        """
        return self._promise.var

    @property
    def promise(self):
        """
        :rtype: flytekit.models.types.OutputReference
        """
        return self._promise

    @property
    def sdk_required(self):
        """
        :rtype: bool
        """
        return self._sdk_required

    @property
    def sdk_default(self):
        """
        :rtype: T
        """
        return self._sdk_default

    @property
    def help(self):
        """
        :rtype: Text
        """
        return self._help

    @property
    def sdk_type(self):
        """
        :rtype: flytekit.common.types.base_sdk_types.FlyteSdkType
        """
        return self._sdk_type

    def __repr__(self):
        return "Input({}, {}, required={}, help={})".format(self.name, self.sdk_type, self.required, self.help)

    @classmethod
    def promote_from_model(cls, model):
        """
        :param flytekit.models.interface.Parameter model:
        :rtype: Input
        """
        sdk_type = _type_helpers.get_sdk_type_from_literal_type(model.var.type)

        if model.default is not None:
            default_value = sdk_type.from_flyte_idl(model.default.to_flyte_idl()).to_python_std()
            return cls("", sdk_type, help=model.var.description, required=False, default=default_value,)
        else:
            return cls("", sdk_type, help=model.var.description, required=True)


class SdkRunnableTask(flytekit.platform.sdk_task.SdkTask, metaclass=_sdk_bases.ExtendedSdkType):
    """
    This class includes the additional logic for building a task that executes in Python code.  It has even more
    validation checks to ensure proper behavior than it's superclasses.

    Since an SdkRunnableTask is assumed to run by hooking into Python code, we will provide additional shortcuts and
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
        environment,
        custom,
    ):
        """
        :param task_function: Function container user code.  This will be executed via the SDK's engine.
        :param Text task_type: string describing the task type
        :param Text discovery_version: string describing the version for task discovery purposes
        :param int retries: Number of retries to attempt
        :param bool interruptible: Specify whether task is interruptible
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
        :param dict[Text, Text] environment:
        :param dict[Text, T] custom:
        """
        # Circular dependency
        from flytekit import __version__

        self._task_function = task_function
        super(SdkRunnableTask, self).__init__(
            task_type,
            _task_models.TaskMetadata(
                discoverable,
                _task_models.RuntimeMetadata(
                    _task_models.RuntimeMetadata.RuntimeType.FLYTE_SDK, __version__, "python",
                ),
                timeout,
                _literal_models.RetryStrategy(retries),
                interruptible,
                discovery_version,
                deprecated,
            ),
            # TODO: If we end up using SdkRunnableTask for the new code, make sure this is set correctly.
            _interface.TypedInterface({}, {}),
            custom,
            container=self._get_container_definition(
                storage_request=storage_request,
                cpu_request=cpu_request,
                gpu_request=gpu_request,
                memory_request=memory_request,
                storage_limit=storage_limit,
                cpu_limit=cpu_limit,
                gpu_limit=gpu_limit,
                memory_limit=memory_limit,
                environment=environment,
            ),
        )
        self.id._name = "{}.{}".format(self.task_module, self.task_function_name)
        self._has_fast_registered = False

    _banned_inputs = {}
    _banned_outputs = {}

    @_exception_scopes.system_entry_point
    def add_inputs(self, inputs):
        """
        Adds the inputs to this task.  This can be called multiple times, but it will fail if an input with a given
        name is added more than once, a name collides with an output, or if the name doesn't exist as an arg name in
        the wrapped function.
        :param dict[Text, flytekit.models.interface.Variable] inputs: names and variables
        """
        self._validate_inputs(inputs)
        self.interface.inputs.update(inputs)

    @classmethod
    def promote_from_model(cls, base_model):
        # TODO: If the task exists in this container, we should be able to retrieve it.
        raise _user_exceptions.FlyteAssertion("Cannot promote a base object to a runnable task.")

    @property
    def task_style(self):
        return self._task_style

    @property
    def task_function(self):
        return self._task_function

    @property
    def task_function_name(self):
        """
        :rtype: Text
        """
        return self.task_function.__name__

    @property
    def task_module(self):
        """
        :rtype: Text
        """
        return self._task_function.__module__

    def validate(self):
        super(SdkRunnableTask, self).validate()
        missing_args = self._missing_mapped_inputs_outputs()
        if len(missing_args) > 0:
            raise _user_exceptions.FlyteAssertion(
                "The task {} is invalid because not all inputs and outputs in the "
                "task function definition were specified in @outputs and @inputs. "
                "We are missing definitions for {}.".format(self, missing_args)
            )

    @_exception_scopes.system_entry_point
    def unit_test(self, **input_map):
        """
        :param dict[Text, T] input_map: Python Std input from users.  We will cast these to the appropriate Flyte
            literals.
        :returns: Depends on the behavior of the specific task in the unit engine.
        """
        return (
            _engine_loader.get_engine("unit")
            .get_task(self)
            .execute(
                _type_helpers.pack_python_std_map_to_literal_map(
                    input_map,
                    {
                        k: _type_helpers.get_sdk_type_from_literal_type(v.type)
                        for k, v in _six.iteritems(self.interface.inputs)
                    },
                )
            )
        )

    @_exception_scopes.system_entry_point
    def local_execute(self, **input_map):
        """
        :param dict[Text, T] input_map: Python Std input from users.  We will cast these to the appropriate Flyte
            literals.
        :rtype: dict[Text, T]
        :returns: The output produced by this task in Python standard format.
        """
        return (
            _engine_loader.get_engine("local")
            .get_task(self)
            .execute(
                _type_helpers.pack_python_std_map_to_literal_map(
                    input_map,
                    {
                        k: _type_helpers.get_sdk_type_from_literal_type(v.type)
                        for k, v in _six.iteritems(self.interface.inputs)
                    },
                )
            )
        )

    def _execute_user_code(self, context, inputs):
        """
        :param flytekit.engines.common.EngineContext context:
        :param dict[Text, T] inputs: This variable is a bit of a misnomer, since it's both inputs and outputs. The
            dictionary passed here will be passed to the user-defined function, and will have values that are a
            variety of types.  The T's here are Python std values for inputs.  If there isn't a native Python type for
            something (like Schema or Blob), they are the Flyte classes.  For outputs they are OutputReferences.
            (Note that these are not the same OutputReferences as in BindingData's)
        :rtype: Any: the returned object from user code.
        :returns: This function must return a dictionary mapping 'filenames' to Flyte Interface Entities.  These
            entities will be used by the engine to pass data from node to node, populate metadata, etc. etc..  Each
            engine will have different behavior.  For instance, the Flyte engine will upload the entities to a remote
            working directory (with the names provided), which will in turn allow Flyte Propeller to push along the
            workflow.  Where as local engine will merely feed the outputs directly into the next node.
        """
        if self.task_style == SdkRunnableTaskStyle.V0:
            return _exception_scopes.user_entry_point(self.task_function)(
                ExecutionParameters(
                    execution_date=context.execution_date,
                    # TODO: it might be better to consider passing the full struct
                    execution_id=_six.text_type(WorkflowExecutionIdentifier.promote_from_model(context.execution_id)),
                    stats=context.stats,
                    logging=context.logging,
                    tmp_dir=context.working_directory,
                ),
                **inputs,
            )

    @_exception_scopes.system_entry_point
    def execute(self, context, inputs):
        """
        :param flytekit.engines.common.EngineContext context:
        :param flytekit.models.literals.LiteralMap inputs:
        :rtype: dict[Text, flytekit.models.common.FlyteIdlEntity]
        :returns: This function must return a dictionary mapping 'filenames' to Flyte Interface Entities.  These
            entities will be used by the engine to pass data from node to node, populate metadata, etc. etc..  Each
            engine will have different behavior.  For instance, the Flyte engine will upload the entities to a remote
            working directory (with the names provided), which will in turn allow Flyte Propeller to push along the
            workflow.  Where as local engine will merely feed the outputs directly into the next node.
        """
        inputs_dict = _type_helpers.unpack_literal_map_to_sdk_python_std(
            inputs, {k: _type_helpers.get_sdk_type_from_literal_type(v.type) for k, v in self.interface.inputs.items()}
        )
        outputs_dict = {
            name: _task_output.OutputReference(_type_helpers.get_sdk_type_from_literal_type(variable.type))
            for name, variable in _six.iteritems(self.interface.outputs)
        }

        # Old style - V0: If annotations are used to define outputs, do not append outputs to the inputs dict
        if not self.task_function.__annotations__ or "return" not in self.task_function.__annotations__:
            inputs_dict.update(outputs_dict)
            self._execute_user_code(context, inputs_dict)
            return {
                _constants.OUTPUT_FILE_NAME: _literal_models.LiteralMap(
                    literals={k: v.sdk_value for k, v in _six.iteritems(outputs_dict)}
                )
            }

    @_exception_scopes.system_entry_point
    def fast_register(self, project, domain, name, digest, additional_distribution, dest_dir) -> str:
        """
        The fast register call essentially hijacks the task container commandline.
        Say an existing task container definition had a commandline like so:
            flyte_venv pyflyte-execute --task-module app.workflows.my_workflow --task-name my_task

        The fast register command introduces a wrapper call to fast-execute the original commandline like so:
            flyte_venv pyflyte-fast-execute --additional-distribution s3://my-s3-bucket/foo/bar/12345.tar.gz --
                flyte_venv pyflyte-execute --task-module app.workflows.my_workflow --task-name my_task

        At execution time pyflyte-fast-execute will ensure the additional distribution (i.e. the fast-registered code)
        exists before calling the original task commandline.

        :param Text project: The project in which to register this task.
        :param Text domain: The domain in which to register this task.
        :param Text name: The name to give this task.
        :param Text digest: The version in which to register this task.
        :param Text additional_distribution: User-specified location for remote source code distribution.
        :param Text The optional location for where to install the additional distribution at runtime
        :rtype: Text: Registered identifier.
        """

        original_container = self.container
        container = _copy.deepcopy(original_container)
        args = ["pyflyte-fast-execute", "--additional-distribution", additional_distribution]
        if dest_dir:
            args += ["--dest-dir", dest_dir]
        args += ["--"] + container.args
        container._args = args
        self._container = container

        try:
            registered_id = self.register(project, domain, name, digest)
        except Exception:
            self._container = original_container
            raise
        self._has_fast_registered = True
        self._container = original_container
        return str(registered_id)

    @property
    def has_fast_registered(self) -> bool:
        return self._has_fast_registered

    def _get_container_definition(
        self,
        storage_request=None,
        cpu_request=None,
        gpu_request=None,
        memory_request=None,
        storage_limit=None,
        cpu_limit=None,
        gpu_limit=None,
        memory_limit=None,
        environment=None,
        cls=None,
    ):
        """
        :param Text storage_request:
        :param Text cpu_request:
        :param Text gpu_request:
        :param Text memory_request:
        :param Text storage_limit:
        :param Text cpu_limit:
        :param Text gpu_limit:
        :param Text memory_limit:
        :param dict[Text,Text] environment:
        :param cls Optional[type]: Type of container to instantiate. Generally should subclass SdkRunnableContainer.
        :rtype: flytekit.models.task.Container
        """
        storage_limit = storage_limit or _resource_config.DEFAULT_STORAGE_LIMIT.get()
        storage_request = storage_request or _resource_config.DEFAULT_STORAGE_REQUEST.get()
        cpu_limit = cpu_limit or _resource_config.DEFAULT_CPU_LIMIT.get()
        cpu_request = cpu_request or _resource_config.DEFAULT_CPU_REQUEST.get()
        gpu_limit = gpu_limit or _resource_config.DEFAULT_GPU_LIMIT.get()
        gpu_request = gpu_request or _resource_config.DEFAULT_GPU_REQUEST.get()
        memory_limit = memory_limit or _resource_config.DEFAULT_MEMORY_LIMIT.get()
        memory_request = memory_request or _resource_config.DEFAULT_MEMORY_REQUEST.get()

        resources = SdkRunnableContainer.get_resources(
            storage_request, cpu_request, gpu_request, memory_request, storage_limit, cpu_limit, gpu_limit, memory_limit
        )

        return (cls or SdkRunnableContainer)(
            command=[],
            args=[
                "pyflyte-execute",
                "--task-module",
                self.task_module,
                "--task-name",
                self.task_function_name,
                "--inputs",
                "{{.input}}",
                "--output-prefix",
                "{{.outputPrefix}}",
                "--raw-output-data-prefix",
                "{{.rawOutputDataPrefix}}",
            ],
            resources=resources,
            env=environment,
            config={},
        )

    def _validate_inputs(self, inputs):
        """
        This method should be overridden in sub-classes that intend to do additional checks on inputs.  If validation
        fails, this function should raise an informative exception.
        :param dict[Text, flytekit.models.interface.Variable] inputs:  Input variables to validate
        :raises: flytekit.common.exceptions.user.FlyteValidationException
        """
        super(SdkRunnableTask, self)._validate_inputs(inputs)
        for k, v in _six.iteritems(inputs):
            if not self._is_argname_in_function_definition(k):
                raise _user_exceptions.FlyteValidationException(
                    "The input named '{}' was not specified in the task function.  Therefore, this input cannot be "
                    "provided to the task.".format(k)
                )
            if _type_helpers.get_sdk_type_from_literal_type(v.type) in type(self)._banned_inputs:
                raise _user_exceptions.FlyteValidationException(
                    "The input '{}' is not an accepted input type.".format(v)
                )

    def _validate_outputs(self, outputs):
        """
        This method should be overridden in sub-classes that intend to do additional checks on outputs.  If validation
        fails, this function should raise an informative exception.
        :param dict[Text, flytekit.models.interface.Variable] outputs:  Output variables to validate
        :raises: flytekit.common.exceptions.user.FlyteValidationException
        """
        super(SdkRunnableTask, self)._validate_outputs(outputs)
        for k, v in _six.iteritems(outputs):
            if not self._is_argname_in_function_definition(k):
                raise _user_exceptions.FlyteValidationException(
                    "The output named '{}' was not specified in the task function.  Therefore, this output cannot be "
                    "provided to the task.".format(k)
                )
            if _type_helpers.get_sdk_type_from_literal_type(v.type) in type(self)._banned_outputs:
                raise _user_exceptions.FlyteValidationException(
                    "The output '{}' is not an accepted output type.".format(v)
                )

    def _get_kwarg_inputs(self):
        # Trim off first parameter as it is reserved for workflow_parameters
        return set(_getargspec(self.task_function).args[1:])

    def _is_argname_in_function_definition(self, key):
        return key in self._get_kwarg_inputs()

    def _missing_mapped_inputs_outputs(self):
        # Trim off first parameter as it is reserved for workflow_parameters
        args = self._get_kwarg_inputs()
        inputs_and_outputs = set(self.interface.outputs.keys()) | set(self.interface.inputs.keys())
        return args ^ inputs_and_outputs