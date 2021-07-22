"""
=====================
Remote Access
=====================

.. currentmodule:: flytekit.remote

This module provides utilities for performing operations on tasks, workflows, launchplans, and executions, for example,
the following code fetches and executes a workflow:

.. code-block:: python

    # create a remote object from environment variables
    remote = FlyteRemote.from_environment()

    # fetch a workflow from the flyte backend
    flyte_workflow = remote.fetch_workflow(name="my_workflow", version="v1")

    # execute the workflow, wait=True will return the execution object after it's completed
    workflow_execution = remote.execute(flyte_workflow, inputs={"a": 1, "b": 10}, wait=True)

    # inspect the execution's outputs
    print(workflow_execution.outputs)

Entrypoint
==========

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   FlyteRemote

Flyte Entities
==============

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   FlyteTask
   FlyteWorkflow
   FlyteLaunchPlan
   FlyteNode
   FlyteTaskNode
   FlyteWorkflowNode
   FlyteWorkflowExecution
   FlyteTaskExecution
   FlyteNodeExecution

"""

from flytekit.remote.component_nodes import FlyteTaskNode, FlyteWorkflowNode
from flytekit.remote.launch_plan import FlyteLaunchPlan
from flytekit.remote.nodes import FlyteNode, FlyteNodeExecution
from flytekit.remote.remote import FlyteRemote
from flytekit.remote.tasks.executions import FlyteTaskExecution
from flytekit.remote.tasks.task import FlyteTask
from flytekit.remote.workflow import FlyteWorkflow
from flytekit.remote.workflow_execution import FlyteWorkflowExecution
