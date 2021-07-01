.. _design-authoring:

############################
Authoring Structure
############################

Enabling users to write tasks and workflows is the core feature of flytekit, it is why it exists. This document goes over how some of the internals work.

*************
Background
*************
Please see the `design doc <https://docs.google.com/document/d/17rNKg6Uvow8CrECaPff96Tarr87P2fn4ilf_Tv2lYd4/edit#>`__.

*********************
Types and Type Engine
*********************
Flyte has its own type system, which is codified `in the IDL <https://github.com/flyteorg/flyteidl>`__.  Python of course has its own typing system, even though it's a dynamic language, and is mostly explained in `PEP 484 <https://www.python.org/dev/peps/pep-0484/>`_. In order to work properly, flytekit needs to be able to convert between the two.

Type Engine
=============
The primary way this happens is through the :py:class:`flytekit.extend.TypeEngine`. This engine works by invoking a series of :py:class:`TypeTransformers <flytekit.extend.TypeTransformer>`. Each transformer is responsible for providing the functionality that the engine needs for a given native Python type.

*****************
Callable Entities
*****************
Tasks, workflows, and launch plans form the core of the Flyte user experience. Each of these concepts are backed by one or more Python classes. These classes in turn, are instantiated by decorators (in the case of tasks and workflow) or a normal Python call (in the case of launch plans).

Tasks
=====
This is the current task class hierarchy.

.. inheritance-diagram:: flytekit.core.python_function_task.PythonFunctionTask flytekit.core.python_function_task.PythonInstanceTask flytekit.extras.sqlite3.task.SQLite3Task
   :parts: 1
   :top-classes: flytekit.core.base_task.Task

Please see the documentation on each of the classes for details.

.. autoclass:: flytekit.core.base_task.Task
   :noindex:

.. autoclass:: flytekit.core.base_task.PythonTask
   :noindex:

.. autoclass:: flytekit.core.python_auto_container.PythonAutoContainerTask
   :noindex:

.. autoclass:: flytekit.core.python_function_task.PythonFunctionTask
   :noindex:

Workflows
=========
There are two workflow classes, which both inherit from the :py:class:`WorkflowBase <flytekit.core.workflow.WorkflowBase>` class.

.. autoclass:: flytekit.core.workflow.PythonFunctionWorkflow
   :noindex:

.. autoclass:: flytekit.core.workflow.ImperativeWorkflow
   :noindex:


Launch Plan
===========
There is also only one :py:class:`LaunchPlan <flytekit.core.launch_plan.LaunchPlan>` class.

.. autoclass:: flytekit.core.launch_plan.LaunchPlan
   :noindex:

**************
Call Patterns
**************
The three entities above are all callable. In Flyte terms that means they can be invoked to yield a unit (or units) of work.
In Python terms that means you can add ``()`` to the end of one of it which invokes the ``__call__`` method on the object.

What happens when a callable entity is called depends on the current context, specifically the current :py:class:`flytekit.FlyteContext`

Raw Task Execution
==================
This is what happens when a task is just run as part of a unit test. The ``@task`` decorator actually turns the decorated function into an instance of the ``PythonFunctionTask`` object but when a user calls it, ``task1()``, outside of a workflow, the original function is called without interference by flytekit.

Task Execution Inside Workflow
==============================
This is what happens, *to the task* when a workflow is being run locally, say as part of a unit test for the workflow.

Before going further, there is a special object that's worth mentioning, the :py:class:`flytekit.extend.Promise`.

.. autoclass:: flytekit.core.promise.Promise
   :noindex:

Let's assume we have a workflow like ::

    @task
    def t1(a: int) -> (int, str):
        return a + 2, "world"

    @task
    def t2(a: str, b: str) -> str:
        return b + a

    @workflow
    def my_wf(a: int, b: str) -> (int, str):
        x, y = t1(a=a).with_overrides(...)
        d = t2(a=y, b=b)
        return x, d

As discussed in the Promise object's documentation, when a task is called from inside a workflow, the Python native values that the raw underlying functions return are first converted into Flyte IDL literals, and then wrapped inside ``Promise`` objects. One ``Promise`` is created for every return variable.

When the next task is called, logic is triggered to unwrap these promises.

Compilation
===========
When a workflow is compiled, instead of producing promise objects that wrap literal values, they wrap a :py:class:`flytekit.core.promise.NodeOutput` instead. This is how data dependency is tracked between tasks.

Branch Skip
===========
If it's been determined that a conditional is not true, then flytekit will skip actually calling the task which means that any side-effects in the task logic will not be run.


.. note::

    Even though in the discussion above, we talked about a task's execution pattern, the same actually applied to workflows and launch plans.
