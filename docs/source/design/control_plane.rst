.. _design-control-plane:

############################
Control Plane Objects
############################
For those who require programmatic access to the control place, certain APIs are available through "control plane classes".

.. note::

    The syntax of this section, while it will continue to work, is subject to change.

Since this section naturally deals with the control plane, this discussion is only relevant for those who have a Flyte backend set up, and have access to it (a local backend will suffice as well of course). These objects do not rely on the underlying code they represent being locally available.

*******
Setup
*******
Similar to the CLIs, this section requires proper configuration. Please follow the setup guide there.

Unlike the CLI case however, you may need to explicitly target the configuration file like so ::

    from flytekit.configuration.common import CONFIGURATION_SINGLETON
    CONFIGURATION_SINGLETON.reset_config("/Users/full/path/to/config")

*******
Classes
*******
This is not an exhaustive list of the objects available, but should provide the reader with the wherewithal to further ascertain for herself additional functionality.

Task
======
To fetch a Task ::

    from flytekit.common.tasks.task import SdkTask
    SdkTask.fetch('flytetester', 'staging', 'recipes.core_basic.task.square', '49b6c6bdbb86e974ffd9875cab1f721ada8066a7')


Workflow
========
To inspect a Workflow ::

    from flytekit.common.workflow import SdkWorkflow
    wf = SdkWorkflow.fetch('flytetester', 'staging', 'recipes.core_basic.basic_workflow.my_wf', '49b6c6bdbb86e974ffd9875cab1f721ada8066a7')

WorkflowExecution
=================
This class represents one run of a workflow.  The ``execution_name`` used here is just the tail end of the URL you see in your browser when looking at a workflow run.

.. code-block:: python

    from flytekit.common.workflow_execution import SdkWorkflowExecution

    e = SdkWorkflowExecution.fetch('project_name', 'development', 'execution_name')
    e.sync()

As a workflow is made up of nodes (each of which can contain a task, a subworkflow, or a launch plan), a workflow execution is made up of node executions (each of which can contain a task execution, a subworkflow execution, or a launch plan execution).
