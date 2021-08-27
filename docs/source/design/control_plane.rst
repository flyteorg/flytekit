.. _design-control-plane:

###################################################
FlyteRemote: A Programmatic Control Plane Interface
###################################################

For those who require programmatic access to the control plane, the :mod:`~flytekit.remote` module enables you to perform
certain operations in a python runtime environment.

Since this section naturally deals with the control plane, this discussion is only relevant for those who have a Flyte
backend set up and have access to it (a :std:ref:`local sandbox <flyte:deployment-sandbox>` will suffice as well).

The video below features a demo of FlyteRemote, followed by all relevant information to create, fetch and execute FlyteRemote objects.

..  youtube:: Vjukdg8LIh8


***************************
Create a FlyteRemote Object
***************************

The :class:`~flytekit.remote.remote.FlyteRemote` class is the entrypoint for programmatically performing operations in a python
runtime. There are two ways of creating a remote object.

**Initialize directly**

.. code-block:: python

    from flytekit.remote import FlyteRemote

    remote = FlyteRemote(
        default_project="project",
        default_domain="domain",
        flyte_admin_url="<url>",
        insecure=True,
    )

**Initialize from flyte config**

.. TODO: link documentation to flyte config and environment variables

This will initialize a :class:`~flytekit.remote.remote.FlyteRemote` object from your flyte config file or environment variable
overrides:

.. code-block:: python

    remote = FlyteRemote.from_config()

*****************************
Fetching Flyte Admin Entities
*****************************

.. code-block:: python

    flyte_task = remote.fetch_task(name="my_task", version="v1")
    flyte_workflow = remote.fetch_workflow(name="my_workflow", version="v1")
    flyte_launch_plan = remote.fetch_launch_plan(name="my_launch_plan", version="v1")

******************
Executing Entities
******************

You can execute all of these flyte entities, which returns a :class:`~flytekit.remote.workflow_execution.FlyteWorkflowExecution` object.
For more information on flyte entities, see the See the :ref:`remote flyte entities <remote-flyte-execution-objects>`
reference.

.. code-block:: python

    flyte_entity = ...  # one of FlyteTask, FlyteWorkflow, or FlyteLaunchPlan
    execution = remote.execute(flyte_entity, inputs={...})

********************************
Waiting for Execution Completion
********************************

You can use the :meth:`~flytekit.remote.remote.FlyteRemote.wait` method to synchronously wait for the execution to complete:

.. code-block:: python

    completed_execution = remote.wait(execution)

You can also pass in ``wait=True`` to the :meth:`~flytekit.remote.remote.FlyteRemote.execute` method.

.. code-block:: python

    completed_execution = remote.execute(flyte_entity, inputs={...}, wait=True)

********************
Syncing Remote State
********************

Use the :meth:`~flytekit.remote.remote.FlyteRemote.sync` method to sync the entity object's state with the remote state

.. code-block:: python

    synced_execution = remote.sync(execution)


****************************
Inspecting Execution Objects
****************************

At any time you can inspect the inputs, outputs, completion status, error status, and other aspects of a workflow
execution object. See the :ref:`remote execution objects <remote-flyte-execution-objects>` reference for a list
of all the available attributes.
