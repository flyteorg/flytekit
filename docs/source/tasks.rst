#######
Tasks
#######

.. automodule:: flytekit

Writing your tasks
********************
Tasks are the building blocks of Flyte. They represent users code. Tasks have the following properties

- Versioned (Usually tied to the git sha)
- optional strong interfaces (specified inputs and outputs)
- declarative
- independently executable
- unit testable


.. contents:: Tasks
    :depth: 2
    :local:
    :backlinks: top

.. currentmodule:: flytekit

.. autosummary::
    :toctree: generated
    :nosignatures:


TaskMetadata
-------------

.. autoclass:: TaskMetadata


Task as a python function
--------------------------

.. autofunction:: task

.. autosummary::
    .. toctree::
        :maxdepth: 2
        :caption: Plugins

        tasks.function.plugins


Specifying Task resources
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: Resources


Reference Tasks
----------------

.. autofunction:: reference_task

Constructing a Reference
^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: TaskReference


Locally Executing tasks
--------------------------

.. code-block:: python

    @task
    def my_task(x: int) -> str:
        return str(x)

    # Now you can invoke a task as a regular function
    my_task(x=10)
    # Note the usage of Keyword arguments. Currently flytekit only supports keyword arguments for tasks


Test utilities when working with a task
----------------------------------------

.. automodule:: flytekit.core.testing

Generate a mock for a task
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autofunction:: task_mock

use mock.patch to patch a task
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autofunction:: patch


.. automodule:: flytekit.core.base_task

Base Task
------------

.. autoclass:: Task

Python Task
------------

.. autoclass:: PythonTask