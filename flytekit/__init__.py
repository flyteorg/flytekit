"""
=====================
Core Flytekit
=====================

.. currentmodule:: flytekit

This package contains all the basic abstractions you'll need to write Flyte.

Basic Authoring
===============

.. autosummary::
   :toctree: generated/

   task
   workflow
   TaskMetadata
   kwtypes
   ExecutionParameters
   FlyteContext
   conditional
   dynamic
   WorkflowFailurePolicy
   logger

Core Task Types
=================

.. autosummary::
   :toctree: generated/

   SQLTask
   ContainerTask
   PythonFunctionTask
   LaunchPlan


Scheduling and Notifications
============================

.. autosummary::
   :toctree: generated/

   Resources
   CronSchedule
   Email
   PagerDuty
   Slack
   FixedRate

Reference Entities
====================

.. autosummary::
   :toctree: generated/

   get_reference_entity
   LaunchPlanReference
   TaskReference
   WorkflowReference
   reference_task
   reference_workflow



"""


import flytekit.plugins  # This will be deprecated, these are the old plugins, the new plugins live in plugins/
from flytekit.core.base_sql_task import SQLTask
from flytekit.core.base_task import TaskMetadata, kwtypes
from flytekit.core.condition import conditional
from flytekit.core.container_task import ContainerTask
from flytekit.core.context_manager import ExecutionParameters, FlyteContext
from flytekit.core.dynamic_workflow_task import dynamic
from flytekit.core.launch_plan import LaunchPlan
from flytekit.core.map_task import maptask
from flytekit.core.notification import Email, PagerDuty, Slack
from flytekit.core.python_function_task import PythonFunctionTask
from flytekit.core.reference import get_reference_entity
from flytekit.core.reference_entity import LaunchPlanReference, TaskReference, WorkflowReference
from flytekit.core.resources import Resources
from flytekit.core.schedule import CronSchedule, FixedRate
from flytekit.core.task import reference_task, task
from flytekit.core.workflow import WorkflowFailurePolicy, reference_workflow, workflow
from flytekit.loggers import logger
from flytekit.types import schema

__version__ = "develop"


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
