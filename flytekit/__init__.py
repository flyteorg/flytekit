"""
=====================
Core Flytekit
=====================

.. currentmodule:: flytekit

This package contains all of the most common abstractions you'll need to write Flyte workflows, and extend Flytekit.

Basic Authoring
===============

These are the essentials needed to get started writing tasks and workflows. The elements here correspond well with :std:ref:`Basic <cookbook:sphx_glr_auto_core_flyte_basics>` section of the cookbook.

.. autosummary::
   :nosignatures:
   :toctree: generated/

   task - This is the basic decorator to use to turn a properly type-annotated function into a Flyte task.
   workflow - This will turn a properly type-annotated function into a Flyte workflow.
   kwtypes - Helper function that makes it slightly easier to declare certain functions.
   current_context - This function returns an ExecutionParameters object.
   ExecutionParameters - This object gives the user at Task run time useful information about the execution environment.
   FlyteContext - This is an flytekit-internal context that is sometimes useful for end-users to have access to.


Running Locally
------------------
Tasks and Workflows can both be locally run (assuming the relevant tasks are capable of local execution). This is useful for unit testing.


Branching and Conditionals
==========================

Branches and conditionals can be expressed explicitly in Flyte. These conditions are evaluated
in the flyte engine and hence should be used for control flow. ``dynamic workflows`` can be used to perform custom conditional logic not supported by flytekit

.. autosummary::
   :nosignatures:
   :toctree: generated/

   conditional


Customizing Tasks & Workflows
==============================

.. autosummary::
   :nosignatures:
   :toctree: generated/

   TaskMetadata - Wrapper object that allows users to specify Task
   Resources - Things like CPUs/Memory, etc.
   WorkflowFailurePolicy - Customizes what happens when a workflow fails.


Dynamic and Nested Workflows
==============================
See the :py:mod:`Dynamic <flytekit.core.dynamic_workflow_task>` module for more information.

.. autosummary::
   :nosignatures:
   :toctree: generated/

   dynamic

Scheduling and Notifications
============================

See the :py:mod:`Notifications Module <flytekit.core.notification>` and
:py:mod:`Schedules Module <flytekit.core.schedule>` for more information.

.. autosummary::
   :nosignatures:
   :toctree: generated/

   CronSchedule
   FixedRate
   Email
   PagerDuty
   Slack

Reference Entities
====================

.. autosummary::
   :nosignatures:
   :toctree: generated/

   get_reference_entity
   LaunchPlanReference
   TaskReference
   WorkflowReference
   reference_task
   reference_workflow

Core Task Types
=================

.. autosummary::
   :nosignatures:
   :toctree: generated/

   SQLTask
   ContainerTask
   PythonFunctionTask
   PythonInstanceTask
   LaunchPlan

Secrets and SecurityContext
============================

.. autosummary::
   :nosignatures:
   :toctree: generated/

   Secret
   SecurityContext

Core Modules
=============

.. autosummary::
   :nosignatures:
   :toctree: generated/

   core.dynamic_workflow_task
   core.notification
   core.schedule

"""


import flytekit.plugins  # This will be deprecated, these are the old plugins, the new plugins live in plugins/
from flytekit.core.base_sql_task import SQLTask
from flytekit.core.base_task import SecurityContext, TaskMetadata, kwtypes
from flytekit.core.condition import conditional
from flytekit.core.container_task import ContainerTask
from flytekit.core.context_manager import ExecutionParameters, FlyteContext
from flytekit.core.dynamic_workflow_task import dynamic
from flytekit.core.launch_plan import LaunchPlan
from flytekit.core.map_task import map_task
from flytekit.core.notification import Email, PagerDuty, Slack
from flytekit.core.python_function_task import PythonFunctionTask, PythonInstanceTask
from flytekit.core.reference import get_reference_entity
from flytekit.core.reference_entity import LaunchPlanReference, TaskReference, WorkflowReference
from flytekit.core.resources import Resources
from flytekit.core.schedule import CronSchedule, FixedRate
from flytekit.core.task import Secret, reference_task, task
from flytekit.core.workflow import ImperativeWorkflow as Workflow
from flytekit.core.workflow import WorkflowFailurePolicy, reference_workflow, workflow
from flytekit.loggers import logger
from flytekit.types import schema

__version__ = "0.0.0+develop"


def current_context() -> ExecutionParameters:
    """
    Use this method to get a handle of specific parameters available in a flyte task.

    Usage

    .. code-block::

        flytekit.current_context().logging.info(...)

    Available params are documented in :py:class:`flytekit.core.context_manager.ExecutionParams`.
    There are some special params, that should be available
    """
    return FlyteContext.current_context().user_space_params
