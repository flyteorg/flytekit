import os
from typing import Optional

from flytekit import current_context
from flytekit.configuration import DataConfig, PlatformConfig, S3Config
from flytekit.core.context_manager import ExecutionState, FlyteContext, FlyteContextManager
from flytekit.loggers import logger
from flytekit.remote import FlyteRemote

FLYTE_SANDBOX_INTERNAL_ENDPOINT = "flyte-sandbox-grpc.flyte:8089"
FLYTE_SANDBOX_MINIO_ENDPOINT = "http://flyte-sandbox-minio.flyte:9000"


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
