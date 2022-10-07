"""
=====================
Core Flytekit
=====================

.. currentmodule:: flytekit

This package contains all of the most common abstractions you'll need to write Flyte workflows and extend Flytekit.

Basic Authoring
===============

These are the essentials needed to get started writing tasks and workflows. The elements here correspond well with :std:ref:`Basics <cookbook:sphx_glr_auto_core_flyte_basics>` section of the user guide.

.. autosummary::
   :nosignatures:
   :template: custom.rst
   :toctree: generated/

   task
   workflow
   kwtypes
   current_context
   ExecutionParameters
   FlyteContext
   map_task
   ~core.workflow.ImperativeWorkflow
   ~core.node_creation.create_node
   FlyteContextManager

Running Locally
------------------
Tasks and Workflows can both be locally run (assuming the relevant tasks are capable of local execution). This is useful for unit testing.


Branching and Conditionals
==========================

Branches and conditionals can be expressed explicitly in Flyte. These conditions are evaluated
in the flyte engine and hence should be used for control flow. ``dynamic workflows`` can be used to perform custom conditional logic not supported by flytekit

.. autosummary::
   :nosignatures:
   :template: custom.rst
   :toctree: generated/

   conditional


Customizing Tasks & Workflows
==============================

.. autosummary::
   :nosignatures:
   :template: custom.rst
   :toctree: generated/

   TaskMetadata - Wrapper object that allows users to specify Task
   Resources - Things like CPUs/Memory, etc.
   WorkflowFailurePolicy - Customizes what happens when a workflow fails.


Dynamic and Nested Workflows
==============================
See the :py:mod:`Dynamic <flytekit.core.dynamic_workflow_task>` module for more information.

.. autosummary::
   :nosignatures:
   :template: custom.rst
   :toctree: generated/

   dynamic

Scheduling
============================

.. autosummary::
   :nosignatures:
   :template: custom.rst
   :toctree: generated/

   CronSchedule
   FixedRate

Notifications
============================

.. autosummary::
   :nosignatures:
   :template: custom.rst
   :toctree: generated/

   Email
   PagerDuty
   Slack

Reference Entities
====================

.. autosummary::
   :nosignatures:
   :template: custom.rst
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
   :template: custom.rst
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
   :template: custom.rst
   :toctree: generated/

   Secret
   SecurityContext


Common Flyte IDL Objects
=========================

.. autosummary::
   :nosignatures:
   :template: custom.rst
   :toctree: generated/

   AuthRole
   Labels
   Annotations
   WorkflowExecutionPhase
   Blob
   BlobMetadata
   Literal
   Scalar
   LiteralType
   BlobType
"""

import sys
from typing import Generator

if sys.version_info < (3, 10):
    from importlib_metadata import entry_points
else:
    from importlib.metadata import entry_points

from flytekit.core.base_sql_task import SQLTask
from flytekit.core.base_task import SecurityContext, TaskMetadata, kwtypes
from flytekit.core.checkpointer import Checkpoint
from flytekit.core.condition import conditional
from flytekit.core.container_task import ContainerTask
from flytekit.core.context_manager import ExecutionParameters, FlyteContext, FlyteContextManager
from flytekit.core.data_persistence import DataPersistence, DataPersistencePlugins
from flytekit.core.dynamic_workflow_task import dynamic
from flytekit.core.hash import HashMethod
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
from flytekit.deck import Deck
from flytekit.extras import pytorch
from flytekit.extras.persistence import GCSPersistence, HttpPersistence, S3Persistence
from flytekit.loggers import logger
from flytekit.models.common import Annotations, AuthRole, Labels
from flytekit.models.core.execution import WorkflowExecutionPhase
from flytekit.models.core.types import BlobType
from flytekit.models.literals import Blob, BlobMetadata, Literal, Scalar
from flytekit.models.types import LiteralType
from flytekit.types import directory, file, numpy, schema
from flytekit.types.structured.structured_dataset import (
    StructuredDataset,
    StructuredDatasetFormat,
    StructuredDatasetTransformerEngine,
    StructuredDatasetType,
)

__version__ = "0.0.0+develop"


def current_context() -> ExecutionParameters:
    """
    Use this method to get a handle of specific parameters available in a flyte task.

    Usage

    .. code-block:: python

        flytekit.current_context().logging.info(...)

    Available params are documented in :py:class:`flytekit.core.context_manager.ExecutionParams`.
    There are some special params, that should be available
    """
    return FlyteContextManager.current_context().execution_state.user_space_params


def new_context() -> Generator[FlyteContext, None, None]:
    return FlyteContextManager.with_context(FlyteContextManager.current_context().new_builder())


def load_implicit_plugins():
    """
    This method allows loading all plugins that have the entrypoint specification. This uses the plugin loading
    behavior as explained `here <>`_.

    This is an opt in system and plugins that have an implicit loading requirement should add the implicit loading
    entrypoint specification to their setup.py. The following example shows how we can autoload a module called fsspec
    (whose init files contains the necessary plugin registration step)

    .. code-block::

        # note the group is always ``flytekit.plugins``
        setup(
        ...
        entry_points={'flytekit.plugins’: 'fsspec=flytekitplugins.fsspec'},
        ...
        )

    This works as long as the fsspec module has

    .. code-block::

       # For data persistence plugins
       DataPersistencePlugins.register_plugin(f"{k}://", FSSpecPersistence, force=True)
       # OR for type plugins
       TypeEngine.register(PanderaTransformer())
       # etc

    """
    discovered_plugins = entry_points(group="flytekit.plugins")
    for p in discovered_plugins:
        p.load()


# Load all implicit plugins
load_implicit_plugins()
