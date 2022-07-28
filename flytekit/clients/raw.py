from __future__ import annotations

import base64 as _base64
import ssl
import subprocess
import time
import typing
from typing import Optional

import grpc
import requests as _requests
from flyteidl.admin.project_pb2 import ProjectListRequest
from flyteidl.service import admin_pb2_grpc as _admin_service
from flyteidl.service import auth_pb2
from flyteidl.service import auth_pb2_grpc as auth_service
from flyteidl.service import dataproxy_pb2 as _dataproxy_pb2
from flyteidl.service import dataproxy_pb2_grpc as dataproxy_service
from flyteidl.service.dataproxy_pb2_grpc import DataProxyServiceStub
from google.protobuf.json_format import MessageToJson as _MessageToJson

from flytekit.clis.auth import credentials as _credentials_access
from flytekit.configuration import AuthType, PlatformConfig
from flytekit.exceptions import user as _user_exceptions
from flytekit.exceptions.user import FlyteAuthenticationException
from flytekit.loggers import cli_logger

_utf_8 = "utf-8"


def _handle_rpc_error(retry=False):
    def decorator(fn):
        def handler(*args, **kwargs):
            """
            Wraps rpc errors as Flyte exceptions and handles authentication the client.
            """
            max_retries = 3
            max_wait_time = 1000

            for i in range(max_retries):
                try:
                    return fn(*args, **kwargs)
                except grpc.RpcError as e:
                    if e.code() == grpc.StatusCode.UNAUTHENTICATED:
                        # Always retry auth errors.
                        if i == (max_retries - 1):
                            # Exit the loop and wrap the authentication error.
                            raise _user_exceptions.FlyteAuthenticationException(str(e))
                        cli_logger.debug(f"Unauthenticated RPC error {e}, refreshing credentials and retrying\n")
                        args[0].refresh_credentials()
                    elif e.code() == grpc.StatusCode.ALREADY_EXISTS:
                        # There are two cases that we should throw error immediately
                        # 1. Entity already exists when we register entity
                        # 2. Entity not found when we fetch entity
                        raise _user_exceptions.FlyteEntityAlreadyExistsException(e)
                    elif e.code() == grpc.StatusCode.NOT_FOUND:
                        raise _user_exceptions.FlyteEntityNotExistException(e)
                    else:
                        # No more retries if retry=False or max_retries reached.
                        if (retry is False) or i == (max_retries - 1):
                            raise
                        else:
                            # Retry: Start with 200ms wait-time and exponentially back-off up to 1 second.
                            wait_time = min(200 * (2**i), max_wait_time)
                            cli_logger.error(f"Non-auth RPC error {e}, sleeping {wait_time}ms and retrying")
                            time.sleep(wait_time / 1000)

        return handler

    return decorator


def _handle_invalid_create_request(fn):
    def handler(self, create_request):
        try:
            fn(self, create_request)
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.INVALID_ARGUMENT:
                cli_logger.error("Error creating Flyte entity because of invalid arguments. Create request: ")
                cli_logger.error(_MessageToJson(create_request))

            # In any case, re-raise since we're not truly handling the error here
            raise e

    return handler


class RawSynchronousFlyteClient(object):
    """
    This is a thin synchronous wrapper around the auto-generated GRPC stubs for communicating with the admin service.

    This client should be usable regardless of environment in which this is used. In other words, configurations should
    be explicit as opposed to inferred from the environment or a configuration file. To create a client,

    .. code-block:: python

        from flytekit.configuration import PlatformConfig
        RawSynchronousFlyteClient(PlatformConfig(endpoint="a.b.com", insecure=True))  # or
        SynchronousFlyteClient(PlatformConfig(endpoint="a.b.com", insecure=True))
    """

    _dataproxy_stub: DataProxyServiceStub

    def __init__(self, cfg: PlatformConfig, **kwargs):
        """
        Initializes a gRPC channel to the given Flyte Admin service.

        Args:
          url: The server address.
          insecure: if insecure is desired
        """
        self._cfg = cfg
        if cfg.insecure:
            self._channel = grpc.insecure_channel(cfg.endpoint, **kwargs)
        elif cfg.insecure_skip_verify:
            # Get port from endpoint or use 443
            endpoint_parts = cfg.endpoint.rsplit(":", 1)
            if len(endpoint_parts) == 2 and endpoint_parts[1].isdigit():
                server_address = tuple(endpoint_parts)
            else:
                server_address = (cfg.endpoint, "443")
            cert = ssl.get_server_certificate(server_address)
            credentials = grpc.ssl_channel_credentials(str.encode(cert))
            options = kwargs.get("options", [])
            self._channel = grpc.secure_channel(
                target=cfg.endpoint,
                credentials=credentials,
                options=options,
                compression=kwargs.get("compression", None),
            )
        else:
            if "credentials" not in kwargs:
                credentials = grpc.ssl_channel_credentials(
                    root_certificates=kwargs.get("root_certificates", None),
                    private_key=kwargs.get("private_key", None),
                    certificate_chain=kwargs.get("certificate_chain", None),
                )
            else:
                credentials = kwargs["credentials"]
            self._channel = grpc.secure_channel(
                target=cfg.endpoint,
                credentials=credentials,
                options=kwargs.get("options", None),
                compression=kwargs.get("compression", None),
            )
        self._stub = _admin_service.AdminServiceStub(self._channel)
        self._auth_stub = auth_service.AuthMetadataServiceStub(self._channel)
        try:
            resp = self._auth_stub.GetPublicClientConfig(auth_pb2.PublicClientAuthConfigRequest())
            self._public_client_config = resp
        except grpc.RpcError:
            cli_logger.debug("No public client auth config found, skipping.")
            self._public_client_config = None
        try:
            resp = self._auth_stub.GetOAuth2Metadata(auth_pb2.OAuth2MetadataRequest())
            self._oauth2_metadata = resp
        except grpc.RpcError:
            cli_logger.debug("No OAuth2 Metadata found, skipping.")
            self._oauth2_metadata = None
        self._dataproxy_stub = dataproxy_service.DataProxyServiceStub(self._channel)

        cli_logger.info(
            f"Flyte Client configured -> {self._cfg.endpoint} in {'insecure' if self._cfg.insecure else 'secure'} mode."
        )
        # metadata will hold the value of the token to send to the various endpoints.
        self._metadata = None

    @classmethod
    def with_root_certificate(cls, cfg: PlatformConfig, root_cert_file: str) -> RawSynchronousFlyteClient:
        b = None
        with open(root_cert_file, "rb") as fp:
            b = fp.read()
        return RawSynchronousFlyteClient(cfg, credentials=grpc.ssl_channel_credentials(root_certificates=b))

    @property
    def public_client_config(self) -> Optional[auth_pb2.PublicClientAuthConfigResponse]:
        return self._public_client_config

    @property
    def oauth2_metadata(self) -> Optional[auth_pb2.OAuth2MetadataResponse]:
        return self._oauth2_metadata

    @property
    def url(self) -> str:
        return self._cfg.endpoint

    def _refresh_credentials_standard(self):
        """
        This function is used when the configuration value for AUTH_MODE is set to 'standard'.
        This either fetches the existing access token or initiates the flow to request a valid access token and store it.
        :param self: RawSynchronousFlyteClient
        :return:
        """
        authorization_header_key = self.public_client_config.authorization_metadata_key or None
        if not self.oauth2_metadata or not self.public_client_config:
            raise ValueError(
                "Raw Flyte client attempting client credentials flow but no response from Admin detected. "
                "Check your Admin server's .well-known endpoints to make sure they're working as expected."
            )

        client = _credentials_access.get_client(
            redirect_endpoint=self.public_client_config.redirect_uri,
            client_id=self.public_client_config.client_id,
            scopes=self.public_client_config.scopes,
            auth_endpoint=self.oauth2_metadata.authorization_endpoint,
            token_endpoint=self.oauth2_metadata.token_endpoint,
        )

        if client.has_valid_credentials and not self.check_access_token(client.credentials.access_token):
            # When Python starts up, if credentials have been stored in the keyring, then the AuthorizationClient
            # will have read them into its _credentials field, but it won't be in the RawSynchronousFlyteClient's
            # metadata field yet. Therefore, if there's a mismatch, copy it over.
            self.set_access_token(client.credentials.access_token, authorization_header_key)
            return

        try:
            client.refresh_access_token()
        except ValueError:
            client.start_authorization_flow()

        self.set_access_token(client.credentials.access_token, authorization_header_key)

    def _refresh_credentials_basic(self):
        """
        This function is used by the _handle_rpc_error() decorator, depending on the AUTH_MODE config object. This handler
        is meant for SDK use-cases of auth (like pyflyte, or when users call SDK functions that require access to Admin,
        like when waiting for another workflow to complete from within a task). This function uses basic auth, which means
        the credentials for basic auth must be present from wherever this code is running.

        :param self: RawSynchronousFlyteClient
        :return:
        """
        if not self.oauth2_metadata or not self.public_client_config:
            raise ValueError(
                "Raw Flyte client attempting client credentials flow but no response from Admin detected. "
                "Check your Admin server's .well-known endpoints to make sure they're working as expected."
            )

        token_endpoint = self.oauth2_metadata.token_endpoint
        scopes = self._cfg.scopes or self.public_client_config.scopes
        scopes = ",".join(scopes)

        # Note that unlike the Pkce flow, the client ID does not come from Admin.
        client_secret = self._cfg.client_credentials_secret
        if not client_secret:
            raise FlyteAuthenticationException("No client credentials secret provided in the config")
        cli_logger.debug(f"Basic authorization flow with client id {self._cfg.client_id} scope {scopes}")
        authorization_header = get_basic_authorization_header(self._cfg.client_id, client_secret)
        token, expires_in = get_token(token_endpoint, authorization_header, scopes)
        cli_logger.info("Retrieved new token, expires in {}".format(expires_in))
        authorization_header_key = self.public_client_config.authorization_metadata_key or None
        self.set_access_token(token, authorization_header_key)

    def _refresh_credentials_from_command(self):
        """
        This function is used when the configuration value for AUTH_MODE is set to 'external_process'.
        It reads an id token generated by an external process started by running the 'command'.

        :param self: RawSynchronousFlyteClient
        :return:
        """

        command = self._cfg.command
        if not command:
            raise FlyteAuthenticationException("No command specified in configuration for command authentication")
        cli_logger.debug("Starting external process to generate id token. Command {}".format(command))
        try:
            output = subprocess.run(command, capture_output=True, text=True, check=True)
        except subprocess.CalledProcessError as e:
            cli_logger.error("Failed to generate token from command {}".format(command))
            raise _user_exceptions.FlyteAuthenticationException("Problems refreshing token with command: " + str(e))
        authorization_header_key = self.public_client_config.authorization_metadata_key or None
        if not authorization_header_key:
            self.set_access_token(output.stdout.strip())
        self.set_access_token(output.stdout.strip(), authorization_header_key)

    def _refresh_credentials_noop(self):
        pass

    def refresh_credentials(self):
        cfg_auth = self._cfg.auth_mode
        if type(cfg_auth) is str:
            try:
                cfg_auth = AuthType[cfg_auth.upper()]
            except KeyError:
                cli_logger.warning(f"Authentication type {cfg_auth} does not exist, defaulting to standard")
                cfg_auth = AuthType.STANDARD

        if cfg_auth == AuthType.STANDARD or cfg_auth == AuthType.PKCE:
            return self._refresh_credentials_standard()
        elif cfg_auth == AuthType.BASIC or cfg_auth == AuthType.CLIENT_CREDENTIALS or cfg_auth == AuthType.CLIENTSECRET:
            return self._refresh_credentials_basic()
        elif cfg_auth == AuthType.EXTERNAL_PROCESS or cfg_auth == AuthType.EXTERNALCOMMAND:
            return self._refresh_credentials_from_command()
        else:
            raise ValueError(
                f"Invalid auth mode [{cfg_auth}] specified." f"Please update the creds config to use a valid value"
            )

    def set_access_token(self, access_token: str, authorization_header_key: Optional[str] = "authorization"):
        # Always set the header to lower-case regardless of what the config is. The grpc libraries that Admin uses
        # to parse the metadata don't change the metadata, but they do automatically lower the key you're looking for.
        cli_logger.debug(f"Adding authorization header. Header name: {authorization_header_key}.")
        self._metadata = [
            (
                authorization_header_key,
                f"Bearer {access_token}",
            )
        ]

    def check_access_token(self, access_token: str) -> bool:
        """
        This checks to see if the given access token is the same as the one already stored in the client. The reason
        this is useful is so that we can prevent unnecessary refreshing of tokens.

        :param access_token: The access token to check
        :return: If no access token is stored, or if the stored token doesn't match, return False.
        """
        if self._metadata is None:
            return False
        return access_token == self._metadata[0][1].replace("Bearer ", "")

    ####################################################################################################################
    #
    #  Task Endpoints
    #
    ####################################################################################################################

    @_handle_rpc_error()
    @_handle_invalid_create_request
    def create_task(self, task_create_request):
        """
        This will create a task definition in the Admin database. Once successful, the task object can be
        retrieved via the client or viewed via the UI or command-line interfaces.

        .. note ::

            Overwrites are not supported so any request for a given project, domain, name, and version that exists in
            the database must match the existing definition exactly. This also means that as long as the request
            remains identical, calling this method multiple times will result in success.

        :param: flyteidl.admin.task_pb2.TaskCreateRequest task_create_request: The request protobuf object.
        :rtype: flyteidl.admin.task_pb2.TaskCreateResponse
        :raises flytekit.common.exceptions.user.FlyteEntityAlreadyExistsException: If an identical version of the task
            is found, this exception is raised.  The client might choose to ignore this exception because the identical
            task is already registered.
        :raises grpc.RpcError:
        """
        return self._stub.CreateTask(task_create_request, metadata=self._metadata)

    @_handle_rpc_error(retry=True)
    def list_task_ids_paginated(self, identifier_list_request):
        """
        This returns a page of identifiers for the tasks for a given project and domain. Filters can also be
        specified.

        .. note ::

            The name field in the TaskListRequest is ignored.

        .. note ::

            This is a paginated API.  Use the token field in the request to specify a page offset token.
            The user of the API is responsible for providing this token.

        .. note ::

            If entries are added to the database between requests for different pages, it is possible to receive
            entries on the second page that also appeared on the first.

        :param: flyteidl.admin.common_pb2.NamedEntityIdentifierListRequest identifier_list_request:
        :rtype: flyteidl.admin.common_pb2.NamedEntityIdentifierList
        :raises: TODO
        """
        return self._stub.ListTaskIds(identifier_list_request, metadata=self._metadata)

    @_handle_rpc_error(retry=True)
    def list_tasks_paginated(self, resource_list_request):
        """
        This returns a page of task metadata for tasks in a given project and domain.  Optionally,
        specifying a name will limit the results to only tasks with that name in the given project and domain.

        .. note ::

            This is a paginated API.  Use the token field in the request to specify a page offset token.
            The user of the API is responsible for providing this token.

        .. note ::

            If entries are added to the database between requests for different pages, it is possible to receive
            entries on the second page that also appeared on the first.

        :param: flyteidl.admin.common_pb2.ResourceListRequest resource_list_request:
        :rtype: flyteidl.admin.task_pb2.TaskList
        :raises: TODO
        """
        return self._stub.ListTasks(resource_list_request, metadata=self._metadata)

    @_handle_rpc_error(retry=True)
    def get_task(self, get_object_request):
        """
        This returns a single task for a given identifier.

        :param: flyteidl.admin.common_pb2.ObjectGetRequest get_object_request:
        :rtype: flyteidl.admin.task_pb2.Task
        :raises: TODO
        """
        return self._stub.GetTask(get_object_request, metadata=self._metadata)

    ####################################################################################################################
    #
    #  Workflow Endpoints
    #
    ####################################################################################################################

    @_handle_rpc_error()
    @_handle_invalid_create_request
    def create_workflow(self, workflow_create_request):
        """
        This will create a workflow definition in the Admin database.  Once successful, the workflow object can be
        retrieved via the client or viewed via the UI or command-line interfaces.

        .. note ::

            Overwrites are not supported so any request for a given project, domain, name, and version that exists in
            the database must match the existing definition exactly.  This also means that as long as the request
            remains identical, calling this method multiple times will result in success.

        :param: flyteidl.admin.workflow_pb2.WorkflowCreateRequest workflow_create_request:
        :rtype: flyteidl.admin.workflow_pb2.WorkflowCreateResponse
        :raises flytekit.common.exceptions.user.FlyteEntityAlreadyExistsException: If an identical version of the
            workflow is found, this exception is raised.  The client might choose to ignore this exception because the
            identical workflow is already registered.
        :raises grpc.RpcError:
        """
        return self._stub.CreateWorkflow(workflow_create_request, metadata=self._metadata)

    @_handle_rpc_error(retry=True)
    def list_workflow_ids_paginated(self, identifier_list_request):
        """
        This returns a page of identifiers for the workflows for a given project and domain. Filters can also be
        specified.

        .. note ::

            The name field in the WorkflowListRequest is ignored.

        .. note ::

            This is a paginated API.  Use the token field in the request to specify a page offset token.
            The user of the API is responsible for providing this token.

        .. note ::

            If entries are added to the database between requests for different pages, it is possible to receive
            entries on the second page that also appeared on the first.

        :param: flyteidl.admin.common_pb2.NamedEntityIdentifierListRequest identifier_list_request:
        :rtype: flyteidl.admin.common_pb2.NamedEntityIdentifierList
        :raises: TODO
        """
        return self._stub.ListWorkflowIds(identifier_list_request, metadata=self._metadata)

    @_handle_rpc_error(retry=True)
    def list_workflows_paginated(self, resource_list_request):
        """
        This returns a page of workflow meta-information for workflows in a given project and domain.  Optionally,
        specifying a name will limit the results to only workflows with that name in the given project and domain.

        .. note ::

            This is a paginated API.  Use the token field in the request to specify a page offset token.
            The user of the API is responsible for providing this token.

        .. note ::

            If entries are added to the database between requests for different pages, it is possible to receive
            entries on the second page that also appeared on the first.

        :param: flyteidl.admin.common_pb2.ResourceListRequest resource_list_request:
        :rtype: flyteidl.admin.workflow_pb2.WorkflowList
        :raises: TODO
        """
        return self._stub.ListWorkflows(resource_list_request, metadata=self._metadata)

    @_handle_rpc_error(retry=True)
    def get_workflow(self, get_object_request):
        """
        This returns a single workflow for a given identifier.

        :param: flyteidl.admin.common_pb2.ObjectGetRequest get_object_request:
        :rtype: flyteidl.admin.workflow_pb2.Workflow
        :raises: TODO
        """
        return self._stub.GetWorkflow(get_object_request, metadata=self._metadata)

    ####################################################################################################################
    #
    #  Launch Plan Endpoints
    #
    ####################################################################################################################

    @_handle_rpc_error()
    @_handle_invalid_create_request
    def create_launch_plan(self, launch_plan_create_request):
        """
        This will create a launch plan definition in the Admin database.  Once successful, the launch plan object can be
        retrieved via the client or viewed via the UI or command-line interfaces.

        .. note ::

            Overwrites are not supported so any request for a given project, domain, name, and version that exists in
            the database must match the existing definition exactly.  This also means that as long as the request
            remains identical, calling this method multiple times will result in success.

        :param: flyteidl.admin.launch_plan_pb2.LaunchPlanCreateRequest launch_plan_create_request:  The request
            protobuf object
        :rtype: flyteidl.admin.launch_plan_pb2.LaunchPlanCreateResponse
        :raises flytekit.common.exceptions.user.FlyteEntityAlreadyExistsException: If an identical version of the
            launch plan is found, this exception is raised.  The client might choose to ignore this exception because
            the identical launch plan is already registered.
        :raises grpc.RpcError:
        """
        return self._stub.CreateLaunchPlan(launch_plan_create_request, metadata=self._metadata)

    # TODO: List endpoints when they come in

    @_handle_rpc_error(retry=True)
    def get_launch_plan(self, object_get_request):
        """
        Retrieves a launch plan entity.

        :param flyteidl.admin.common_pb2.ObjectGetRequest object_get_request:
        :rtype: flyteidl.admin.launch_plan_pb2.LaunchPlan
        """
        return self._stub.GetLaunchPlan(object_get_request, metadata=self._metadata)

    @_handle_rpc_error(retry=True)
    def get_active_launch_plan(self, active_launch_plan_request):
        """
        Retrieves a launch plan entity.

        :param flyteidl.admin.common_pb2.ActiveLaunchPlanRequest active_launch_plan_request:
        :rtype: flyteidl.admin.launch_plan_pb2.LaunchPlan
        """
        return self._stub.GetActiveLaunchPlan(active_launch_plan_request, metadata=self._metadata)

    @_handle_rpc_error()
    def update_launch_plan(self, update_request):
        """
        Allows updates to a launch plan at a given identifier.  Currently, a launch plan may only have it's state
        switched between ACTIVE and INACTIVE.

        :param flyteidl.admin.launch_plan_pb2.LaunchPlanUpdateRequest update_request:
        :rtype: flyteidl.admin.launch_plan_pb2.LaunchPlanUpdateResponse
        """
        return self._stub.UpdateLaunchPlan(update_request, metadata=self._metadata)

    @_handle_rpc_error(retry=True)
    def list_launch_plan_ids_paginated(self, identifier_list_request):
        """
        Lists launch plan named identifiers for a given project and domain.

        :param: flyteidl.admin.common_pb2.NamedEntityIdentifierListRequest identifier_list_request:
        :rtype: flyteidl.admin.common_pb2.NamedEntityIdentifierList
        """
        return self._stub.ListLaunchPlanIds(identifier_list_request, metadata=self._metadata)

    @_handle_rpc_error(retry=True)
    def list_launch_plans_paginated(self, resource_list_request):
        """
        Lists Launch Plans for a given Identifier (project, domain, name)

        :param: flyteidl.admin.common_pb2.ResourceListRequest resource_list_request:
        :rtype: flyteidl.admin.launch_plan_pb2.LaunchPlanList
        """
        return self._stub.ListLaunchPlans(resource_list_request, metadata=self._metadata)

    @_handle_rpc_error(retry=True)
    def list_active_launch_plans_paginated(self, active_launch_plan_list_request):
        """
        Lists Active Launch Plans for a given (project, domain)

        :param: flyteidl.admin.common_pb2.ActiveLaunchPlanListRequest active_launch_plan_list_request:
        :rtype: flyteidl.admin.launch_plan_pb2.LaunchPlanList
        """
        return self._stub.ListActiveLaunchPlans(active_launch_plan_list_request, metadata=self._metadata)

    ####################################################################################################################
    #
    #  Named Entity Endpoints
    #
    ####################################################################################################################

    @_handle_rpc_error()
    def update_named_entity(self, update_named_entity_request):
        """
        :param flyteidl.admin.common_pb2.NamedEntityUpdateRequest update_named_entity_request:
        :rtype: flyteidl.admin.common_pb2.NamedEntityUpdateResponse
        """
        return self._stub.UpdateNamedEntity(update_named_entity_request, metadata=self._metadata)

    ####################################################################################################################
    #
    #  Workflow Execution Endpoints
    #
    ####################################################################################################################

    @_handle_rpc_error()
    def create_execution(self, create_execution_request):
        """
        This will create an execution for the given execution spec.
        :param flyteidl.admin.execution_pb2.ExecutionCreateRequest create_execution_request:
        :rtype: flyteidl.admin.execution_pb2.ExecutionCreateResponse
        """
        return self._stub.CreateExecution(create_execution_request, metadata=self._metadata)

    @_handle_rpc_error()
    def recover_execution(self, recover_execution_request):
        """
        This will recreate an execution with the same spec as the one belonging to the given execution identifier.
        :param flyteidl.admin.execution_pb2.ExecutionRecoverRequest recover_execution_request:
        :rtype: flyteidl.admin.execution_pb2.ExecutionRecoverResponse
        """
        return self._stub.RecoverExecution(recover_execution_request, metadata=self._metadata)

    @_handle_rpc_error(retry=True)
    def get_execution(self, get_object_request):
        """
        Returns an execution of a workflow entity.

        :param flyteidl.admin.execution_pb2.WorkflowExecutionGetRequest get_object_request:
        :rtype: flyteidl.admin.execution_pb2.Execution
        """
        return self._stub.GetExecution(get_object_request, metadata=self._metadata)

    @_handle_rpc_error(retry=True)
    def get_execution_data(self, get_execution_data_request):
        """
        Returns signed URLs to LiteralMap blobs for an execution's inputs and outputs (when available).

        :param flyteidl.admin.execution_pb2.WorkflowExecutionGetRequest get_execution_data_request:
        :rtype: flyteidl.admin.execution_pb2.WorkflowExecutionGetDataResponse
        """
        return self._stub.GetExecutionData(get_execution_data_request, metadata=self._metadata)

    @_handle_rpc_error(retry=True)
    def list_executions_paginated(self, resource_list_request):
        """
        Lists the executions for a given identifier.

        :param flyteidl.admin.common_pb2.ResourceListRequest resource_list_request:
        :rtype: flyteidl.admin.execution_pb2.ExecutionList
        """
        return self._stub.ListExecutions(resource_list_request, metadata=self._metadata)

    @_handle_rpc_error()
    def terminate_execution(self, terminate_execution_request):
        """
        :param flyteidl.admin.execution_pb2.TerminateExecutionRequest terminate_execution_request:
        :rtype: flyteidl.admin.execution_pb2.TerminateExecutionResponse
        """
        return self._stub.TerminateExecution(terminate_execution_request, metadata=self._metadata)

    @_handle_rpc_error()
    def relaunch_execution(self, relaunch_execution_request):
        """
        :param flyteidl.admin.execution_pb2.ExecutionRelaunchRequest relaunch_execution_request:
        :rtype: flyteidl.admin.execution_pb2.ExecutionCreateResponse
        """
        return self._stub.RelaunchExecution(relaunch_execution_request, metadata=self._metadata)

    ####################################################################################################################
    #
    #  Node Execution Endpoints
    #
    ####################################################################################################################

    @_handle_rpc_error(retry=True)
    def get_node_execution(self, node_execution_request):
        """
        :param flyteidl.admin.node_execution_pb2.NodeExecutionGetRequest node_execution_request:
        :rtype: flyteidl.admin.node_execution_pb2.NodeExecution
        """
        return self._stub.GetNodeExecution(node_execution_request, metadata=self._metadata)

    @_handle_rpc_error(retry=True)
    def get_node_execution_data(self, get_node_execution_data_request):
        """
        Returns signed URLs to LiteralMap blobs for a node execution's inputs and outputs (when available).

        :param flyteidl.admin.node_execution_pb2.NodeExecutionGetDataRequest get_node_execution_data_request:
        :rtype: flyteidl.admin.node_execution_pb2.NodeExecutionGetDataResponse
        """
        return self._stub.GetNodeExecutionData(get_node_execution_data_request, metadata=self._metadata)

    @_handle_rpc_error(retry=True)
    def list_node_executions_paginated(self, node_execution_list_request):
        """
        :param flyteidl.admin.node_execution_pb2.NodeExecutionListRequest node_execution_list_request:
        :rtype: flyteidl.admin.node_execution_pb2.NodeExecutionList
        """
        return self._stub.ListNodeExecutions(node_execution_list_request, metadata=self._metadata)

    @_handle_rpc_error(retry=True)
    def list_node_executions_for_task_paginated(self, node_execution_for_task_list_request):
        """
        :param flyteidl.admin.node_execution_pb2.NodeExecutionListRequest node_execution_for_task_list_request:
        :rtype: flyteidl.admin.node_execution_pb2.NodeExecutionList
        """
        return self._stub.ListNodeExecutionsForTask(node_execution_for_task_list_request, metadata=self._metadata)

    ####################################################################################################################
    #
    #  Task Execution Endpoints
    #
    ####################################################################################################################

    @_handle_rpc_error(retry=True)
    def get_task_execution(self, task_execution_request):
        """
        :param flyteidl.admin.task_execution_pb2.TaskExecutionGetRequest task_execution_request:
        :rtype: flyteidl.admin.task_execution_pb2.TaskExecution
        """
        return self._stub.GetTaskExecution(task_execution_request, metadata=self._metadata)

    @_handle_rpc_error(retry=True)
    def get_task_execution_data(self, get_task_execution_data_request):
        """
        Returns signed URLs to LiteralMap blobs for a task execution's inputs and outputs (when available).

        :param flyteidl.admin.task_execution_pb2.TaskExecutionGetDataRequest get_task_execution_data_request:
        :rtype: flyteidl.admin.task_execution_pb2.TaskExecutionGetDataResponse
        """
        return self._stub.GetTaskExecutionData(get_task_execution_data_request, metadata=self._metadata)

    @_handle_rpc_error(retry=True)
    def list_task_executions_paginated(self, task_execution_list_request):
        """
        :param flyteidl.admin.task_execution_pb2.TaskExecutionListRequest task_execution_list_request:
        :rtype: flyteidl.admin.task_execution_pb2.TaskExecutionList
        """
        return self._stub.ListTaskExecutions(task_execution_list_request, metadata=self._metadata)

    ####################################################################################################################
    #
    #  Project Endpoints
    #
    ####################################################################################################################

    @_handle_rpc_error(retry=True)
    def list_projects(self, project_list_request: typing.Optional[ProjectListRequest] = None):
        """
        This will return a list of the projects registered with the Flyte Admin Service
        :param flyteidl.admin.project_pb2.ProjectListRequest project_list_request:
        :rtype: flyteidl.admin.project_pb2.Projects
        """
        if project_list_request is None:
            project_list_request = ProjectListRequest()
        return self._stub.ListProjects(project_list_request, metadata=self._metadata)

    @_handle_rpc_error()
    def register_project(self, project_register_request):
        """
        Registers a project along with a set of domains.
        :param flyteidl.admin.project_pb2.ProjectRegisterRequest project_register_request:
        :rtype: flyteidl.admin.project_pb2.ProjectRegisterResponse
        """
        return self._stub.RegisterProject(project_register_request, metadata=self._metadata)

    @_handle_rpc_error()
    def update_project(self, project):
        """
        Update an existing project specified by id.
        :param flyteidl.admin.project_pb2.Project project:
        :rtype: flyteidl.admin.project_pb2.ProjectUpdateResponse
        """
        return self._stub.UpdateProject(project, metadata=self._metadata)

    ####################################################################################################################
    #
    #  Matching Attributes Endpoints
    #
    ####################################################################################################################
    @_handle_rpc_error()
    def update_project_domain_attributes(self, project_domain_attributes_update_request):
        """
        This updates the attributes for a project and domain registered with the Flyte Admin Service
        :param flyteidl.admin.ProjectDomainAttributesUpdateRequest project_domain_attributes_update_request:
        :rtype: flyteidl.admin.ProjectDomainAttributesUpdateResponse
        """
        return self._stub.UpdateProjectDomainAttributes(
            project_domain_attributes_update_request, metadata=self._metadata
        )

    @_handle_rpc_error()
    def update_workflow_attributes(self, workflow_attributes_update_request):
        """
        This updates the attributes for a project, domain, and workflow registered with the Flyte Admin Service
        :param flyteidl.admin.UpdateWorkflowAttributesRequest workflow_attributes_update_request:
        :rtype: flyteidl.admin.WorkflowAttributesUpdateResponse
        """
        return self._stub.UpdateWorkflowAttributes(workflow_attributes_update_request, metadata=self._metadata)

    @_handle_rpc_error(retry=True)
    def get_project_domain_attributes(self, project_domain_attributes_get_request):
        """
        This fetches the attributes for a project and domain registered with the Flyte Admin Service
        :param flyteidl.admin.ProjectDomainAttributesGetRequest project_domain_attributes_get_request:
        :rtype: flyteidl.admin.ProjectDomainAttributesGetResponse
        """
        return self._stub.GetProjectDomainAttributes(project_domain_attributes_get_request, metadata=self._metadata)

    @_handle_rpc_error(retry=True)
    def get_workflow_attributes(self, workflow_attributes_get_request):
        """
        This fetches the attributes for a project, domain, and workflow registered with the Flyte Admin Service
        :param flyteidl.admin.GetWorkflowAttributesAttributesRequest workflow_attributes_get_request:
        :rtype: flyteidl.admin.WorkflowAttributesGetResponse
        """
        return self._stub.GetWorkflowAttributes(workflow_attributes_get_request, metadata=self._metadata)

    @_handle_rpc_error(retry=True)
    def list_matchable_attributes(self, matchable_attributes_list_request):
        """
        This fetches the attributes for a specific resource type registered with the Flyte Admin Service
        :param flyteidl.admin.ListMatchableAttributesRequest matchable_attributes_list_request:
        :rtype: flyteidl.admin.ListMatchableAttributesResponse
        """
        return self._stub.ListMatchableAttributes(matchable_attributes_list_request, metadata=self._metadata)

    ####################################################################################################################
    #
    #  Event Endpoints
    #
    ####################################################################################################################

    # TODO: (P2) Implement the event endpoints in case there becomes a use-case for third-parties to submit events
    # through the client in Python.

    ####################################################################################################################
    #
    #  Data proxy endpoints
    #
    ####################################################################################################################
    @_handle_rpc_error(retry=True)
    def create_upload_location(
        self, create_upload_location_request: _dataproxy_pb2.CreateUploadLocationRequest
    ) -> _dataproxy_pb2.CreateUploadLocationResponse:
        """
        Get a signed url to be used during fast registration
        :param flyteidl.service.dataproxy_pb2.CreateUploadLocationRequest create_upload_location_request:
        :rtype: flyteidl.service.dataproxy_pb2.CreateUploadLocationResponse
        """
        return self._dataproxy_stub.CreateUploadLocation(create_upload_location_request, metadata=self._metadata)

    @_handle_rpc_error(retry=True)
    def create_download_location(
        self, create_download_location_request: _dataproxy_pb2.CreateDownloadLocationRequest
    ) -> _dataproxy_pb2.CreateDownloadLocationResponse:
        """
        Get a signed url to be used during fast registration
        :param flyteidl.service.dataproxy_pb2.CreateDownloadLocationRequest create_download_location_request:
        :rtype: flyteidl.service.dataproxy_pb2.CreateDownloadLocationResponse
        """
        return self._dataproxy_stub.CreateDownloadLocation(create_download_location_request, metadata=self._metadata)


def get_token(token_endpoint, authorization_header, scope):
    """
    :param Text token_endpoint:
    :param Text authorization_header: This is the value for the "Authorization" key. (eg 'Bearer abc123')
    :param Text scope:
    :rtype: (Text,Int) The first element is the access token retrieved from the IDP, the second is the expiration
            in seconds
    """
    headers = {
        "Authorization": authorization_header,
        "Cache-Control": "no-cache",
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    body = {
        "grant_type": "client_credentials",
    }
    if scope is not None:
        body["scope"] = scope
    response = _requests.post(token_endpoint, data=body, headers=headers)
    if response.status_code != 200:
        cli_logger.error("Non-200 ({}) received from IDP: {}".format(response.status_code, response.text))
        raise FlyteAuthenticationException("Non-200 received from IDP")

    response = response.json()
    return response["access_token"], response["expires_in"]


def get_basic_authorization_header(client_id, client_secret):
    """
    This function transforms the client id and the client secret into a header that conforms with http basic auth.
    It joins the id and the secret with a : then base64 encodes it, then adds the appropriate text.
    :param Text client_id:
    :param Text client_secret:
    :rtype: Text
    """
    concated = "{}:{}".format(client_id, client_secret)
    return "Basic {}".format(_base64.b64encode(concated.encode(_utf_8)).decode(_utf_8))
