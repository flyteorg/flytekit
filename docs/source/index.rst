.. simpleble documentation master file, created by
   sphinx-quickstart on Fri Mar  9 04:07:53 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

*************************
Flytekit Python Reference
*************************

This section of the documentation provides detailed descriptions of the high-level design of ``Flytekit`` and an
API reference for specific usage details of Python functions, classes, and decorators that you import to specify tasks,
build workflows, and extend ``Flytekit``.

Installation
============

.. code-block::

   pip install flytekit

For developer environment setup instructions, see the :ref:`contributor guide <contributing>`.


Quickstart
==========

.. code-block:: python

   from flytekit import task, workflow

   @task
   def sum(x: int, y: int) -> int:
      return x + y

   @task
   def square(z: int) -> int:
      return z * z

   @workflow
   def my_workflow(x: int, y: int) -> int:
      return sum(x=square(z=x), y=square(z=y))

   print(f"my_workflow output: {my_workflow(x=1, y=2)}")


Expected output:

.. code-block::

   my_workflow output: 5


.. toctree::
   :maxdepth: 1
   :hidden:

   |plane| Getting Started <https://docs.flyte.org/en/latest/getting_started.html>
   |book-reader| User Guide <https://docs.flyte.org/projects/cookbook/en/latest/index.html>
   |chalkboard| Tutorials <https://docs.flyte.org/projects/cookbook/en/latest/tutorials.html>
   |project-diagram| Concepts <https://docs.flyte.org/en/latest/concepts/basics.html>
   |rocket| Deployment <https://docs.flyte.org/en/latest/deployment/index.html>
   |book| API Reference <https://docs.flyte.org/en/latest/reference/index.html>
   |hands-helping| Community <https://docs.flyte.org/en/latest/community/index.html>

.. NOTE: The caption text is important for the Sphinx theme to correctly render the nav header
.. https://github.com/flyteorg/furo
.. toctree::
   :maxdepth: -1
   :caption: Flytekit SDK
   :hidden:

   Flytekit Python <self>
   design/index
   flytekit
   remote
   testing
   extend
   tasks.extend
   types.extend
   data.extend
   contributing
