####################
Accelerator support
####################

.. tags:: MachineLearning, Advanced, Hardware

Flyte tasks are very powerful and allow you to select `gpu` resources for your task. However, there are some cases where
you may want to use a different accelerator type, such as a TPU or specific variations of GPUs or even use fractional GPU's.
Flyte makes it possible to configure the backend to utilize different accelerators and this module provides a way for
the user to request for these specific accelerators. The module provides some constant for known accelerators, but
remember this is not a complete list. If you know the name of the accelerator you want to use, you can simply pass the
string name to the task.

.. code-block::

    from flytekit.extras.accelerators import T4

    @task(
        limits=Resources(gpu="1"),
        accelerator=T4,
    )
    def my_task() -> None:
        ...


.. automodule:: flytekit.extras.accelerators
   :members:
   :undoc-members:
   :show-inheritance:
