from flytekit.common.exceptions import user as _user


class RecoverableException(_user.FlyteRecoverableException):
    """
    Raise an exception of this type if user code detects an error and would like to force a retry of the entire task.
    Any exception raised from user code other than RecoverableException will NOT be considered retryable and the task
    will fail without additional retries.
    """

    pass
