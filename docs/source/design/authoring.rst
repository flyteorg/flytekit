.. _design-authoring:

#######################
Authoring Structure
#######################

One of the core features of Flytekit is to enable users to write tasks and workflows. In this section, we will understand how it works internally.

*************
Background
*************
Please refer to the `design doc <https://docs.google.com/document/d/17rNKg6Uvow8CrECaPff96Tarr87P2fn4ilf_Tv2lYd4/edit#>`__.

*********************
Types and Type Engine
*********************
Flyte has its own type system which is codified `in the IDL <https://github.com/flyteorg/flyteidl>`__.  Python has its own typing system, even though it is a dynamic language, and is mostly explained in `PEP 484 <https://www.python.org/dev/peps/pep-0484/>`_. In order to work properly, Flytekit needs to convert between the two type systems.

Type Engine
=============
This primariliy happens through the :py:class:`flytekit.extend.TypeEngine`. This engine works by invoking a series of :py:class:`TypeTransformers <flytekit.extend.TypeTransformer>`. Each transformer is responsible for providing the functionality that the engine requires for a given native Python type.

*****************
Callable Entities
*****************
Tasks, workflows, and launch plans form the core of the Flyte user experience. Each of these concepts are backed by one or more Python classes. These classes in turn, are instantiated by decorators (in the case of tasks and workflow) or a normal Python call (in the case of launch plans).

Tasks
=====
This is the current task class hierarchy:

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
==========
There are two workflow classes, and both inherit from the :py:class:`WorkflowBase <flytekit.core.workflow.WorkflowBase>` class.

.. autoclass:: flytekit.core.workflow.PythonFunctionWorkflow
   :noindex:

.. autoclass:: flytekit.core.workflow.ImperativeWorkflow
   :noindex:


Launch Plan
===========
There is only one :py:class:`LaunchPlan <flytekit.core.launch_plan.LaunchPlan>` class.

.. autoclass:: flytekit.core.launch_plan.LaunchPlan
   :noindex:

.. exception_handling:

******************
Exception Handling
******************
Exception handling takes place along two dimensions:

* System vs User: We try to differentiate between user exceptions and Flytekit/system level exceptions. For instance, if Flytekit fails to upload its outputs, that's a system exception. If the user raises a ``ValueError`` because of unexpected input in the task code, that's a user exception.
* Recoverable vs Non-recoverable: Recoverable errors will be retried and count against your task's retry count. Non-recoverable errors will simply fail. System exceptions are by default recoverable (since there's a good chance it was just a blip).

This is the user exception tree. Feel free to raise any of these exception classes. Note that the ``FlyteRecoverableException`` is the only recoverable exception. All others, along with all non-Flytekit defined exceptions, are non-recoverable.

.. inheritance-diagram:: flytekit.common.exceptions.user.FlyteValidationException flytekit.common.exceptions.user.FlyteEntityAlreadyExistsException flytekit.common.exceptions.user.FlyteValueException flytekit.common.exceptions.user.FlyteTimeout flytekit.common.exceptions.user.FlyteAuthenticationException flytekit.common.exceptions.user.FlyteRecoverableException
   :parts: 1
   :top-classes: Exception

Implementation
==============
For those who want to dig deeper, take a look at the :py:class:`flytekit.common.exceptions.scopes.FlyteScopedException` classes.
There are also two decorators which you'll find interspersed throughout the codebase.

.. autofunction:: flytekit.common.exceptions.scopes.system_entry_point

.. autofunction:: flytekit.common.exceptions.scopes.user_entry_point

**************
Call Patterns
**************
The above mentioned entities (tasks, workflows, and launch plan) are all callable. In terms of Flyte, it means they can be invoked to yield a unit (or units) of work.
In Pythonic term, it means you can add ``()`` to the end of one of the entities which invokes the ``__call__`` method on the object.

What happens when a callable entity is called depends on the current context, specifically the current :py:class:`flytekit.FlyteContext`

Raw Task Execution
===================
This is what happens when a task is just run as part of a unit test. The ``@task`` decorator actually turns the decorated function into an instance of the ``PythonFunctionTask`` object but when a user calls it, ``task1()``, outside of a workflow, the original function is called without interference by Flytekit.

Task Execution Inside Workflow
===============================
When a workflow is run locally, as a part of a unit test, certain changes occur in the task.

Before going further, there is a special object that's worth mentioning, the :py:class:`flytekit.extend.Promise`.

.. autoclass:: flytekit.core.promise.Promise
   :noindex:

Let's assume we have a workflow like ::

    @task
    def t1(a: int) -> Tuple[int, str]:
        return a + 2, "world"

    @task
    def t2(a: str, b: str) -> str:
        return b + a

    @workflow
    def my_wf(a: int, b: str) -> Tuple[int, str]:
        x, y = t1(a=a).with_overrides(...)
        d = t2(a=y, b=b)
        return x, d

As discussed in the Promise object's documentation, when a task is called from inside a workflow, the Python native values returned by the raw underlying functions are first converted into Flyte IDL literals, and then wrapped inside ``Promise`` objects. One ``Promise`` is created for every return variable.

When the next task is called, the logic is triggered to unwrap these Promises.

Compilation
===========
When a workflow is compiled, instead of producing Promise objects that wrap literal values, they wrap a :py:class:`flytekit.core.promise.NodeOutput` instead. This helps track data dependency between tasks.

Branch Skip
===========
If a conditional is determined to be false, then Flytekit will skip calling the task. This avoids running the faulty task logic.


.. note::

    In the discussion above, we discussed about a task's execution pattern. The same can be applied to workflows and launch plans too!
