.. _design-authoring-tasks:

############################
Flytekit Tasks Structure
############################

*********
Diagram
*********

This is what the current structure of Task related classes looks like ::

                                            +--------+
                                            |  Task  |
                                            +----|---+
                                                 |
                                                 |
                                         +-------v------+
                           +-------------| PythonTask   |--------------+
                           |             +--------------+              |
                           |                 |                         |
                           |                 |                         |
                           v                 v                         v
                    +-------------+ +--------------------------+  +---------------+
                 +--|   SQLTask   | | PythonAutoContainerTask  |  | ContainerTask |
                 |  +------+------+ +--------------------------+  +---------------+
                 |         |          |                     |
                 |         |          |                     |
                 v         |          v                     v
            +----------+   |  +--------------------+   +---------------------+
            | HiveTask |   |  | PythonFunctionTask |   | PythonInstanceTask  |
            +----------+   |  +--------------------+   +---------------------+
                           |                              |
                           v                              |
                 +--------------+                         |
                 | SQLite3Task  |<------------------------+
                 +--------------+


************
Descriptions
************

Please see the documentation on each of the classes for details.

.. autoclass:: flytekit.core.base_task.Task
   :noindex:

.. autoclass:: flytekit.core.base_task.PythonTask
   :noindex:

.. autoclass:: flytekit.core.python_function_task.PythonAutoContainerTask
   :noindex:

.. autoclass:: flytekit.core.python_function_task.PythonFunctionTask
   :noindex:

