import abc as _abc

from flytekit.models import common as _common_models


class BaseWorkflowExecutor(object, metaclass=_common_models.FlyteABCMeta):
    """
    This class must be implemented for any engine to create, interact with, and execute workflows using the
    FlyteKit SDK.
    """

    def __init__(self, sdk_workflow):
        """
        :param flytekit.common.workflow.SdkWorkflow sdk_workflow:
        """
        self._sdk_workflow = sdk_workflow

    @property
    def sdk_workflow(self):
        """
        :rtype: flytekit.common.workflow.SdkWorkflow
        """
        return self._sdk_workflow

    @_abc.abstractmethod
    def register(self, identifier):
        """
        Registers the workflow
        :param flytekit.models.core.identifier.Identifier identifier:
        """
        pass


class BaseWorkflowExecution(object, metaclass=_common_models.FlyteABCMeta):
    """
    This class must be implemented for any engine to track and interact with the executions of workflows.
    """

    def __init__(self, sdk_wf_exec):
        """
        :param flytekit.common.workflow_execution.SdkWorkflowExecution sdk_wf_exec:
        """
        self._sdk_wf_exec = sdk_wf_exec

    @property
    def sdk_workflow_execution(self):
        """
        :rtype: flytekit.common.workflow_execution.SdkWorkflowExecution
        """
        return self._sdk_wf_exec

    @_abc.abstractmethod
    def get_node_executions(self, filters=None):
        """
        :param list[flytekit.models.filters.Filter] filters:
        :rtype: dict[Text, flytekit.common.nodes.SdkNodeExecution]
        """
        pass

    @_abc.abstractmethod
    def sync(self):
        """
        :rtype: None
        """
        pass

    @_abc.abstractmethod
    def get_inputs(self):
        """
        :rtype: flytekit.models.literals.LiteralMap
        """
        pass

    @_abc.abstractmethod
    def get_outputs(self):
        """
        :rtype: flytekit.models.literals.LiteralMap
        """
        pass

    @_abc.abstractmethod
    def terminate(self, cause):
        """
        :param Text cause:
        """
        pass


class BaseNodeExecution(object, metaclass=_common_models.FlyteABCMeta):
    def __init__(self, node_execution):
        """
        :param flytekit.common.nodes.SdkNodeExecution node_execution:
        """
        self._sdk_node_execution = node_execution

    @property
    def sdk_node_execution(self):
        """
        :rtype: flytekit.common.nodes.SdkNodeExecution
        """
        return self._sdk_node_execution

    @_abc.abstractmethod
    def get_task_executions(self):
        """
        :rtype: list[flytekit.common.tasks.executions.SdkTaskExecution]
        """
        pass

    @_abc.abstractmethod
    def get_subworkflow_executions(self):
        """
        :rtype: list[flytekit.common.workflow_execution.SdkWorkflowExecution]
        """
        pass

    @_abc.abstractmethod
    def get_inputs(self):
        """
        :rtype: flytekit.models.literals.LiteralMap
        """
        pass

    @_abc.abstractmethod
    def get_outputs(self):
        """
        :rtype: flytekit.models.literals.LiteralMap
        """
        pass

    @_abc.abstractmethod
    def sync(self):
        """
        :rtype: None
        """
        pass


class BaseTaskExecution(object, metaclass=_common_models.FlyteABCMeta):
    def __init__(self, task_exec):
        """
        :param flytekit.common.tasks.executions.SdkTaskExecution task_exec:
        """
        self._sdk_task_execution = task_exec

    @property
    def sdk_task_execution(self):
        """
        :rtype: flytekit.common.tasks.executions.SdkTaskExecution
        """
        return self._sdk_task_execution

    @_abc.abstractmethod
    def get_inputs(self):
        """
        :rtype: flytekit.models.literals.LiteralMap
        """
        pass

    @_abc.abstractmethod
    def get_outputs(self):
        """
        :rtype: flytekit.models.literals.LiteralMap
        """
        pass

    @_abc.abstractmethod
    def sync(self):
        """
        :rtype: None
        """
        pass

    @_abc.abstractmethod
    def get_child_executions(self, filters=None):
        """
        :param list[flytekit.models.filters.Filter] filters:
        :rtype: dict[Text, flytekit.common.nodes.SdkNodeExecution]
        """
        pass


class BaseLaunchPlanLauncher(object, metaclass=_common_models.FlyteABCMeta):
    def __init__(self, sdk_launch_plan):
        """
        :param flytekit.common.launch_plan.SdkLaunchPlan sdk_launch_plan:
        """
        self._sdk_launch_plan = sdk_launch_plan

    @property
    def sdk_launch_plan(self):
        """
        :rtype: flytekit.common.launch_plan.SdkLaunchPlan
        """
        return self._sdk_launch_plan

    @_abc.abstractmethod
    def register(self, identifier):
        """
        Registers the launch plan
        :param flytekit.models.core.identifier.Identifier identifier:
        """
        pass

    @_abc.abstractmethod
    def launch(
        self,
        project,
        domain,
        name,
        inputs,
        notification_overrides=None,
        label_overrides=None,
        annotation_overrides=None,
    ):
        """
        Registers the launch plan and returns the identifier.
        :param Text project:
        :param Text domain:
        :param Text name:
        :param flytekit.models.literals.LiteralMap inputs: The inputs to pass
        :param list[flytekit.models.common.Notification] notification_overrides: If specified, override the
            notifications.
        :param flytekit.models.common.Labels label_overrides:
        :param flytekit.models.common.Annotations annotation_overrides:
        :rtype: flytekit.models.execution.Execution
        """
        pass

    @_abc.abstractmethod
    def update(self, identifier, state):
        """
        :param flytekit.models.core.identifier.Identifier identifier: ID for launch plan to update
        :param int state: Enum value from flytekit.models.launch_plan.LaunchPlanState
        """
        pass


class BaseTaskExecutor(object, metaclass=_common_models.FlyteABCMeta):
    def __init__(self, sdk_task):
        """
        :param flytekit.common.tasks.task.SdkTask sdk_task:
        """
        self._sdk_task = sdk_task

    @property
    def sdk_task(self):
        """
        :rtype: flytekit.common.tasks.sdk_runnable.SdkRunnableTask
        """
        return self._sdk_task

    @_abc.abstractmethod
    def execute(self, inputs, context=None):
        """
        :param flytekit.models.literals.LiteralMap inputs: Inputs to pass to the workflow.
        """
        pass

    @_abc.abstractmethod
    def register(self, identifier):
        """
        Registers the task
        :param flytekit.models.core.identifier.Identifier identifier:
        """
        pass

    @_abc.abstractmethod
    def launch(
        self,
        project,
        domain,
        name=None,
        inputs=None,
        notification_overrides=None,
        label_overrides=None,
        annotation_overrides=None,
        auth_role=None,
    ):
        """
        Executes the task as a single task execution and returns the identifier.
        :param Text project:
        :param Text domain:
        :param Text name:
        :param flytekit.models.literals.LiteralMap inputs: The inputs to pass
        :param list[flytekit.models.common.Notification] notification_overrides: If specified, override the
            notifications.
        :param flytekit.models.common.Labels label_overrides:
        :param flytekit.models.common.Annotations annotation_overrides:
        :param flytekit.models.common.AuthRole auth_role:
        :rtype: flytekit.models.execution.Execution
        """
        pass


class BaseExecutionEngineFactory(object, metaclass=_common_models.FlyteABCMeta):
    """
    This object should be implemented to satisfy the basic engine interface.
    """

    @_abc.abstractmethod
    def get_task(self, sdk_task):
        """
        :param flytekit.common.tasks.task.SdkTask sdk_task:
        :rtype: BaseTaskExecutor
        """
        pass

    @_abc.abstractmethod
    def get_launch_plan(self, sdk_launch_plan):
        """
        :param flytekit.common.launch_plan.SdkLaunchPlan sdk_launch_plan:
        :rtype: BaseLaunchPlanLauncher
        """
        pass

    @_abc.abstractmethod
    def get_task_execution(self, task_exec):
        """
        :param flytekit.common.tasks.executions.SdkTaskExecution task_exec:
        :rtype: BaseTaskExecution
        """
        pass

    @_abc.abstractmethod
    def get_node_execution(self, node_exec):
        """
        :param flytekit.common.nodes.SdkNodeExecution node_exec:
        :rtype: BaseNodeExecution
        """
        pass

    @_abc.abstractmethod
    def get_workflow_execution(self, wf_exec):
        """
        :param flytekit.common.workflow_execution.SdkWorkflowExecution wf_exec:
        :rtype: BaseWorkflowExecution
        """
        pass

    @_abc.abstractmethod
    def fetch_workflow_execution(self, wf_exec_id):
        """
        :param flytekit.models.core.identifier.WorkflowExecutionIdentifier wf_exec_id:
        :rtype: flytekit.models.execution.Execution
        """
        pass

    @_abc.abstractmethod
    def fetch_task(self, task_id):
        """
        :param flytekit.models.core.identifier.Identifier task_id: This identifier should have a resource type of kind
            Task.
        :rtype: flytekit.models.task.Task
        """
        pass

    @_abc.abstractmethod
    def fetch_latest_task(self, named_task):
        """
        Fetches the latest task
        :param flytekit.models.common.NamedEntityIdentifier named_task: NamedEntityIdentifier to fetch
        :rtype: flytekit.models.task.Task
        """
        pass


class EngineContext(object):
    def __init__(
        self,
        execution_date,
        tmp_dir,
        stats,
        execution_id,
        logging,
        raw_output_data_prefix=None,
    ):
        self._stats = stats
        self._execution_date = execution_date
        self._working_directory = tmp_dir
        self._execution_id = execution_id
        self._logging = logging
        self._raw_output_data_prefix = raw_output_data_prefix

    @property
    def stats(self):
        """
        :rtype: flytekit.interfaces.stats.taggable.TaggableStats
        """
        return self._stats

    @property
    def logging(self):
        """
        :rtype: TODO
        """
        return self._logging

    @property
    def working_directory(self):
        """
        :rtype: flytekit.common.utils.AutoDeletingTempDir
        """
        return self._working_directory

    @property
    def execution_date(self):
        """
        :rtype: datetime.datetime
        """
        return self._execution_date

    @property
    def execution_id(self):
        """
        :rtype: flytekit.models.core.identifier.WorkflowExecutionIdentifier
        """
        return self._execution_id

    @property
    def raw_output_data_prefix(self) -> str:
        return self._raw_output_data_prefix
