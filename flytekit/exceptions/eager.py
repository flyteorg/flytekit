class EagerException(Exception):
    """Raised when a node in an eager workflow encounters an error.

    This exception should be used in an :py:func:`@eager <flytekit.core.task.eager>` workflow function to
    catch exceptions that are raised by tasks or subworkflows.

    .. code-block:: python

        from flytekit import task
        from flytekit.exceptions.eager import EagerException

        @task
        def add_one(x: int) -> int:
            if x < 0:
                raise ValueError("x must be positive")
            return x + 1

        @task
        def double(x: int) -> int:
            return x * 2

        @eager
        async def eager_workflow(x: int) -> int:
            try:
                out = await add_one(x=x)
            except EagerException:
                # The ValueError error is caught
                # and raised as an EagerException
                raise
            return await double(x=out)
    """
