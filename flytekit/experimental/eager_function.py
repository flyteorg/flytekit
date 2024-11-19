import os
from functools import partial
from typing import Optional

from flytekit import current_context
from flytekit.configuration import DataConfig, PlatformConfig, S3Config
from flytekit.core.context_manager import ExecutionState, FlyteContext, FlyteContextManager
from flytekit.core.python_function_task import EagerAsyncPythonFunctionTask
from flytekit.loggers import logger
from flytekit.remote import FlyteRemote

FLYTE_SANDBOX_INTERNAL_ENDPOINT = "flyte-sandbox-grpc.flyte:8089"
FLYTE_SANDBOX_MINIO_ENDPOINT = "http://flyte-sandbox-minio.flyte:9000"


# async def terminate(self):
#     execution = self.remote.sync(self._execution)
#     logger.debug(f"Cleaning up execution: {execution}")
#     if not execution.is_done:
#         self.remote.terminate(
#             execution,
#             f"Execution terminated by eager workflow execution {self.async_stack.parent_execution_id}.",
#         )
#
#         poll_interval = self._poll_interval or timedelta(seconds=6)
#         time_to_give_up = (
#             (datetime.max.replace(tzinfo=timezone.utc))
#             if self._timeout is None
#             else datetime.now(timezone.utc) + self._timeout
#         )
#
#         while datetime.now(timezone.utc) < time_to_give_up:
#             execution = self.remote.sync(execution)
#             if execution.is_done:
#                 break
#             await asyncio.sleep(poll_interval.total_seconds())


def eager(
    _fn=None,
    *args,
    **kwargs,
) -> EagerAsyncPythonFunctionTask:
    """Eager workflow decorator.

    :param remote: A :py:class:`~flytekit.remote.FlyteRemote` object to use for executing Flyte entities.
    :param client_secret_group: The client secret group to use for this workflow.
    :param client_secret_key: The client secret key to use for this workflow.
    :param timeout: The timeout duration specifying how long to wait for a task/workflow execution within the eager
        workflow to complete or terminate. By default, the eager workflow will wait indefinitely until complete.
    :param poll_interval: The poll interval for checking if a task/workflow execution within the eager workflow has
        finished. If not specified, the default poll interval is 6 seconds.
    :param local_entrypoint: If True, the eager workflow will can be executed locally but use the provided
        :py:func:`~flytekit.remote.FlyteRemote` object to create task/workflow executions. This is useful for local
        testing against a remote Flyte cluster.
    :param client_secret_env_var: if specified, binds the client secret to the specified environment variable for
        remote authentication.
    :param kwargs: keyword-arguments forwarded to :py:func:`~flytekit.task`.

    This type of workflow will execute all flyte entities within it eagerly, meaning that all python constructs can be
    used inside of an ``@eager``-decorated function. This is because eager workflows use a
    :py:class:`~flytekit.remote.remote.FlyteRemote` object to kick off executions when a flyte entity needs to produce a
    value.

    For example:

    .. code-block:: python

        from flytekit import task
        from flytekit.experimental import eager

        @task
        def add_one(x: int) -> int:
            return x + 1

        @task
        def double(x: int) -> int:
            return x * 2

        @eager
        async def eager_workflow(x: int) -> int:
            out = await add_one(x=x)
            return await double(x=out)

        # run locally with asyncio
        if __name__ == "__main__":
            import asyncio

            result = asyncio.run(eager_workflow(x=1))
            print(f"Result: {result}")  # "Result: 4"

    Unlike :py:func:`dynamic workflows <flytekit.dynamic>`, eager workflows are not compiled into a workflow spec, but
    uses python's `async <https://docs.python.org/3/library/asyncio.html>`__ capabilities to execute flyte entities.

    .. note::

       Eager workflows only support `@task`, `@workflow`, and `@eager` entities. Dynamic workflows and launchplans are
       currently not supported.

    Note that for the ``@eager`` function is an ``async`` function. Under the hood, tasks and workflows called inside
    an ``@eager`` workflow are executed asynchronously. This means that task and workflow calls will return an awaitable,
    which need to be awaited.

    .. important::

       A ``client_secret_group`` and ``client_secret_key`` is needed for authenticating via
       :py:class:`~flytekit.remote.remote.FlyteRemote` using the ``client_credentials`` authentication, which is
       configured via :py:class:`~flytekit.configuration.PlatformConfig`.

       .. code-block:: python

            from flytekit.remote import FlyteRemote
            from flytekit.configuration import Config

            @eager(
                remote=FlyteRemote(config=Config.auto(config_file="config.yaml")),
                client_secret_group="my_client_secret_group",
                client_secret_key="my_client_secret_key",
            )
            async def eager_workflow(x: int) -> int:
                out = await add_one(x)
                return await double(one)

       Where ``config.yaml`` contains is a flytectl-compatible config file.
       For more details, see `here <https://docs.flyte.org/en/latest/flytectl/overview.html#configuration>`__.

       When using a sandbox cluster started with ``flytectl demo start``, however, the ``client_secret_group``
       and ``client_secret_key`` are not needed, :

       .. code-block:: python

            @eager(remote=FlyteRemote(config=Config.for_sandbox()))
            async def eager_workflow(x: int) -> int:
                ...

    .. important::

       When using ``local_entrypoint=True`` you also need to specify the ``remote`` argument. In this case, the eager
       workflow runtime will be local, but all task/subworkflow invocations will occur on the specified Flyte cluster.
       This argument is primarily used for testing and debugging eager workflow logic locally.

    """

    if _fn is None:
        return partial(
            eager,
            **kwargs,
        )

    if "enable_deck" in kwargs:
        del kwargs["enable_deck"]

    et = EagerAsyncPythonFunctionTask(task_config=None, task_function=_fn, enable_deck=True, **kwargs)
    return et


def _prepare_remote(
    remote: Optional[FlyteRemote],
    ctx: FlyteContext,
    client_secret_group: Optional[str] = None,
    client_secret_key: Optional[str] = None,
    local_entrypoint: bool = False,
    client_secret_env_var: Optional[str] = None,
) -> Optional[FlyteRemote]:
    """Prepare FlyteRemote object for accessing Flyte cluster in a task running on the same cluster."""

    is_local_execution_mode = ctx.execution_state.mode in {
        ExecutionState.Mode.LOCAL_TASK_EXECUTION,
        ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION,
    }

    if remote is not None and local_entrypoint and is_local_execution_mode:
        # when running eager workflows as a local entrypoint, we don't have to modify the remote object
        # because we can assume that the user is running this from their local machine and can do browser-based
        # authentication.
        logger.info("Running eager workflow as local entrypoint")
        return remote

    if remote is None or is_local_execution_mode:
        # if running the "eager workflow" (which is actually task) locally, run the task as a function,
        # which doesn't need a remote object
        return None

    # Handle the case where this the task is running in a Flyte cluster and needs to access the cluster itself
    # via FlyteRemote.
    if remote.config.platform.endpoint.startswith("localhost"):
        # replace sandbox endpoints with internal dns, since localhost won't exist within the Flyte cluster
        return _internal_demo_remote(remote)
    return _internal_remote(remote, client_secret_group, client_secret_key, client_secret_env_var)


def _internal_demo_remote(remote: FlyteRemote) -> FlyteRemote:
    """Derives a FlyteRemote object from a sandbox yaml configuration, modifying parts to make it work internally."""
    # replace sandbox endpoints with internal dns, since localhost won't exist within the Flyte cluster
    return FlyteRemote(
        config=remote.config.with_params(
            platform=PlatformConfig(
                endpoint=FLYTE_SANDBOX_INTERNAL_ENDPOINT,
                insecure=True,
            ),
            data_config=DataConfig(
                s3=S3Config(
                    endpoint=FLYTE_SANDBOX_MINIO_ENDPOINT,
                    access_key_id=remote.config.data_config.s3.access_key_id,
                    secret_access_key=remote.config.data_config.s3.secret_access_key,
                ),
            ),
        ),
        default_domain=remote.default_domain,
        default_project=remote.default_project,
    )


def _internal_remote(
    remote: FlyteRemote,
    client_secret_group: Optional[str],
    client_secret_key: Optional[str],
    client_secret_env_var: Optional[str],
) -> FlyteRemote:
    """Derives a FlyteRemote object from a yaml configuration file, modifying parts to make it work internally."""
    secrets_manager = current_context().secrets

    assert (
        client_secret_group is not None or client_secret_key is not None
    ), "One of client_secret_group or client_secret_key must be defined when using a remote cluster"

    client_secret = secrets_manager.get(client_secret_group, client_secret_key)
    # get the raw output prefix from the context that's set from the pyflyte-execute entrypoint
    # (see flytekit/bin/entrypoint.py)

    if client_secret_env_var is not None:
        # this creates a remote client where the env var client secret is sufficient for authentication
        os.environ[client_secret_env_var] = client_secret
        try:
            remote_cls = type(remote)
            return remote_cls(
                default_domain=remote.default_domain,
                default_project=remote.default_project,
            )
        except Exception as exc:
            raise TypeError(f"Unable to authenticate remote class {remote_cls} with client secret") from exc

    ctx = FlyteContextManager.current_context()
    return FlyteRemote(
        config=remote.config.with_params(
            platform=PlatformConfig(
                endpoint=remote.config.platform.endpoint,
                insecure=remote.config.platform.insecure,
                auth_mode="client_credentials",
                client_id=remote.config.platform.client_id,
                client_credentials_secret=remote.config.platform.client_credentials_secret or client_secret,
            ),
        ),
        default_domain=remote.default_domain,
        default_project=remote.default_project,
        data_upload_location=ctx.file_access.raw_output_prefix,
    )
