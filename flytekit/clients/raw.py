import logging as _logging

import six as _six
from flyteidl.service import admin_pb2_grpc as _admin_service
from google.protobuf.json_format import MessageToJson as _MessageToJson
from grpc import RpcError as _RpcError
from grpc import StatusCode as _GrpcStatusCode
from grpc import insecure_channel as _insecure_channel
from grpc import secure_channel as _secure_channel
from grpc import ssl_channel_credentials as _ssl_channel_credentials

from flytekit.clis.auth import credentials as _credentials_access
from flytekit.clis.sdk_in_container import basic_auth as _basic_auth
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.configuration import creds as _creds_config
from flytekit.configuration.creds import CLIENT_CREDENTIALS_SCOPE as _SCOPE
from flytekit.configuration.creds import CLIENT_ID as _CLIENT_ID
from flytekit.configuration.platform import AUTH as _AUTH


def _refresh_credentials_standard(flyte_client):
    """
    This function is used when the configuration value for AUTH_MODE is set to 'standard'.
    This either fetches the existing access token or initiates the flow to request a valid access token and store it.
    :param flyte_client: RawSynchronousFlyteClient
    :return:
    """

    _credentials_access.get_client(flyte_client.url).refresh_access_token()
    flyte_client.set_access_token(_credentials_access.get_client(flyte_client.url).credentials.access_token)


def _refresh_credentials_basic(flyte_client):
    """
    This function is used by the _handle_rpc_error decorator, depending on the AUTH_MODE config object. This handler
    is meant for SDK use-cases of auth (like pyflyte, or when users call SDK functions that require access to Admin,
    like when waiting for another workflow to complete from within a task). This function uses basic auth, which means
    the credentials for basic auth must be present from wherever this code is running.

    :param flyte_client: RawSynchronousFlyteClient
    :return:
    """
    auth_endpoints = _credentials_access.get_authorization_endpoints(flyte_client.url)
    token_endpoint = auth_endpoints.token_endpoint
    client_secret = _basic_auth.get_secret()
    _logging.debug("Basic authorization flow with client id {} scope {}".format(_CLIENT_ID.get(), _SCOPE.get()))
    authorization_header = _basic_auth.get_basic_authorization_header(_CLIENT_ID.get(), client_secret)
    token, expires_in = _basic_auth.get_token(token_endpoint, authorization_header, _SCOPE.get())
    _logging.info("Retrieved new token, expires in {}".format(expires_in))
    flyte_client.set_access_token(token)


def _refresh_credentials_noop(flyte_client):
    pass


def _get_refresh_handler(auth_mode):
    if auth_mode == "standard":
        return _refresh_credentials_standard
    elif auth_mode == "basic":
        return _refresh_credentials_basic
    else:
        raise ValueError(
            "Invalid auth mode [{}] specified. Please update the creds config to use a valid value".format(auth_mode)
        )


def _handle_rpc_error(fn):
    def handler(*args, **kwargs):
        """
        Wraps rpc errors as Flyte exceptions and handles authentication the client.
        :param args:
        :param kwargs:
        :return:
        """
        retries = 2
        try:
            for i in range(retries):
                try:
                    return fn(*args, **kwargs)
                except _RpcError as e:
                    if e.code() == _GrpcStatusCode.UNAUTHENTICATED:
                        if i == (retries - 1):
                            # Exit the loop and wrap the authentication error.
                            raise _user_exceptions.FlyteAuthenticationException(_six.text_type(e))
                        refresh_handler_fn = _get_refresh_handler(_creds_config.AUTH_MODE.get())
                        refresh_handler_fn(args[0])
                    else:
                        raise
        except _RpcError as e:
            if e.code() == _GrpcStatusCode.ALREADY_EXISTS:
                raise _user_exceptions.FlyteEntityAlreadyExistsException(_six.text_type(e))
            else:
                raise

    return handler


def _handle_invalid_create_request(fn):
    def handler(self, create_request):
        try:
            fn(self, create_request)
        except _RpcError as e:
            if e.code() == _GrpcStatusCode.INVALID_ARGUMENT:
                _logging.error("Error creating Flyte entity because of invalid arguments. Create request: ")
                _logging.error(_MessageToJson(create_request))

            # In any case, re-raise since we're not truly handling the error here
            raise e

    return handler


class RawSynchronousFlyteClient(object):
    """
    This is a thin synchronous wrapper around the auto-generated GRPC stubs for communicating with the admin service.

    This client should be usable regardless of environment in which this is used. In other words, configurations should
    be explicit as opposed to inferred from the environment or a configuration file.
    """

    def __init__(self, url, insecure=False, credentials=None, options=None):
        """
        Initializes a gRPC channel to the given Flyte Admin service.

        :param Text url: The URL (including port if necessary) to connect to the appropriate Flyte Admin Service.
        :param bool insecure: [Optional] Whether to use an insecure connection, default False
        :param Text credentials: [Optional] If provided, a secure channel will be opened with the Flyte Admin Service.
        :param dict[Text, Text] options: [Optional] A dict of key-value string pairs for configuring the gRPC core
            runtime.
        :param list[(Text,Text)] metadata: [Optional] metadata pairs to be transmitted to the
            service-side of the RPC.
        """
        self._channel = None
        self._url = url

        if insecure:
            self._channel = _insecure_channel(url, options=list((options or {}).items()))
        else:
            self._channel = _secure_channel(
                url, credentials or _ssl_channel_credentials(), options=list((options or {}).items()),
            )
        self._stub = _admin_service.AdminServiceStub(self._channel)
        self._metadata = None
        if _AUTH.get():
            self.force_auth_flow()

    @property
    def url(self) -> str:
        return self._url

    def set_access_token(self, access_token):
        # Always set the header to lower-case regardless of what the config is. The grpc libraries that Admin uses
        # to parse the metadata don't change the metadata, but they do automatically lower the key you're looking for.
        self._metadata = [(_creds_config.AUTHORIZATION_METADATA_KEY.get().lower(), "Bearer {}".format(access_token),)]

    def force_auth_flow(self):
        refresh_handler_fn = _get_refresh_handler(_creds_config.AUTH_MODE.get())
        refresh_handler_fn(self)

    ####################################################################################################################
    #
    #  Task Endpoints
    #
    ####################################################################################################################

    @_handle_rpc_error
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

    @_handle_rpc_error
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

    @_handle_rpc_error
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

    @_handle_rpc_error
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

    @_handle_rpc_error
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

    @_handle_rpc_error
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

    @_handle_rpc_error
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

    @_handle_rpc_error
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

    @_handle_rpc_error
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

    @_handle_rpc_error
    def get_launch_plan(self, object_get_request):
        """
        Retrieves a launch plan entity.

        :param flyteidl.admin.common_pb2.ObjectGetRequest object_get_request:
        :rtype: flyteidl.admin.launch_plan_pb2.LaunchPlan
        """
        return self._stub.GetLaunchPlan(object_get_request, metadata=self._metadata)

    @_handle_rpc_error
    def get_active_launch_plan(self, active_launch_plan_request):
        """
        Retrieves a launch plan entity.

        :param flyteidl.admin.common_pb2.ActiveLaunchPlanRequest active_launch_plan_request:
        :rtype: flyteidl.admin.launch_plan_pb2.LaunchPlan
        """
        return self._stub.GetActiveLaunchPlan(active_launch_plan_request, metadata=self._metadata)

    @_handle_rpc_error
    def update_launch_plan(self, update_request):
        """
        Allows updates to a launch plan at a given identifier.  Currently, a launch plan may only have it's state
        switched between ACTIVE and INACTIVE.

        :param flyteidl.admin.launch_plan_pb2.LaunchPlanUpdateRequest update_request:
        :rtype: flyteidl.admin.launch_plan_pb2.LaunchPlanUpdateResponse
        """
        return self._stub.UpdateLaunchPlan(update_request, metadata=self._metadata)

    @_handle_rpc_error
    def list_launch_plan_ids_paginated(self, identifier_list_request):
        """
        Lists launch plan named identifiers for a given project and domain.

        :param: flyteidl.admin.common_pb2.NamedEntityIdentifierListRequest identifier_list_request:
        :rtype: flyteidl.admin.common_pb2.NamedEntityIdentifierList
        """
        return self._stub.ListLaunchPlanIds(identifier_list_request, metadata=self._metadata)

    @_handle_rpc_error
    def list_launch_plans_paginated(self, resource_list_request):
        """
        Lists Launch Plans for a given Identifer (project, domain, name)

        :param: flyteidl.admin.common_pb2.ResourceListRequest resource_list_request:
        :rtype: flyteidl.admin.launch_plan_pb2.LaunchPlanList
        """
        return self._stub.ListLaunchPlans(resource_list_request, metadata=self._metadata)

    @_handle_rpc_error
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

    @_handle_rpc_error
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

    @_handle_rpc_error
    def create_execution(self, create_execution_request):
        """
        This will create an execution for the given execution spec.
        :param flyteidl.admin.execution_pb2.ExecutionCreateRequest create_execution_request:
        :rtype: flyteidl.admin.execution_pb2.ExecutionCreateResponse
        """
        return self._stub.CreateExecution(create_execution_request, metadata=self._metadata)

    @_handle_rpc_error
    def get_execution(self, get_object_request):
        """
        Returns an execution of a workflow entity.

        :param flyteidl.admin.execution_pb2.WorkflowExecutionGetRequest get_object_request:
        :rtype: flyteidl.admin.execution_pb2.Execution
        """
        return self._stub.GetExecution(get_object_request, metadata=self._metadata)

    @_handle_rpc_error
    def get_execution_data(self, get_execution_data_request):
        """
        Returns signed URLs to LiteralMap blobs for an execution's inputs and outputs (when available).

        :param flyteidl.admin.execution_pb2.WorkflowExecutionGetRequest get_execution_data_request:
        :rtype: flyteidl.admin.execution_pb2.WorkflowExecutionGetDataResponse
        """
        return self._stub.GetExecutionData(get_execution_data_request, metadata=self._metadata)

    @_handle_rpc_error
    def list_executions_paginated(self, resource_list_request):
        """
        Lists the executions for a given identifier.

        :param flyteidl.admin.common_pb2.ResourceListRequest resource_list_request:
        :rtype: flyteidl.admin.execution_pb2.ExecutionList
        """
        return self._stub.ListExecutions(resource_list_request, metadata=self._metadata)

    @_handle_rpc_error
    def terminate_execution(self, terminate_execution_request):
        """
        :param flyteidl.admin.execution_pb2.TerminateExecutionRequest terminate_execution_request:
        :rtype: flyteidl.admin.execution_pb2.TerminateExecutionResponse
        """
        return self._stub.TerminateExecution(terminate_execution_request, metadata=self._metadata)

    @_handle_rpc_error
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

    @_handle_rpc_error
    def get_node_execution(self, node_execution_request):
        """
        :param flyteidl.admin.node_execution_pb2.NodeExecutionGetRequest node_execution_request:
        :rtype: flyteidl.admin.node_execution_pb2.NodeExecution
        """
        return self._stub.GetNodeExecution(node_execution_request, metadata=self._metadata)

    @_handle_rpc_error
    def get_node_execution_data(self, get_node_execution_data_request):
        """
        Returns signed URLs to LiteralMap blobs for a node execution's inputs and outputs (when available).

        :param flyteidl.admin.node_execution_pb2.NodeExecutionGetDataRequest get_node_execution_data_request:
        :rtype: flyteidl.admin.node_execution_pb2.NodeExecutionGetDataResponse
        """
        return self._stub.GetNodeExecutionData(get_node_execution_data_request, metadata=self._metadata)

    @_handle_rpc_error
    def list_node_executions_paginated(self, node_execution_list_request):
        """
        :param flyteidl.admin.node_execution_pb2.NodeExecutionListRequest node_execution_list_request:
        :rtype: flyteidl.admin.node_execution_pb2.NodeExecutionList
        """
        return self._stub.ListNodeExecutions(node_execution_list_request, metadata=self._metadata)

    @_handle_rpc_error
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

    @_handle_rpc_error
    def get_task_execution(self, task_execution_request):
        """
        :param flyteidl.admin.task_execution_pb2.TaskExecutionGetRequest task_execution_request:
        :rtype: flyteidl.admin.task_execution_pb2.TaskExecution
        """
        return self._stub.GetTaskExecution(task_execution_request, metadata=self._metadata)

    @_handle_rpc_error
    def get_task_execution_data(self, get_task_execution_data_request):
        """
        Returns signed URLs to LiteralMap blobs for a task execution's inputs and outputs (when available).

        :param flyteidl.admin.task_execution_pb2.TaskExecutionGetDataRequest get_task_execution_data_request:
        :rtype: flyteidl.admin.task_execution_pb2.TaskExecutionGetDataResponse
        """
        return self._stub.GetTaskExecutionData(get_task_execution_data_request, metadata=self._metadata)

    @_handle_rpc_error
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

    @_handle_rpc_error
    def list_projects(self, project_list_request):
        """
        This will return a list of the projects registered with the Flyte Admin Service
        :param flyteidl.admin.project_pb2.ProjectListRequest project_list_request:
        :rtype: flyteidl.admin.project_pb2.Projects
        """
        return self._stub.ListProjects(project_list_request, metadata=self._metadata)

    @_handle_rpc_error
    def register_project(self, project_register_request):
        """
        Registers a project along with a set of domains.
        :param flyteidl.admin.project_pb2.ProjectRegisterRequest project_register_request:
        :rtype: flyteidl.admin.project_pb2.ProjectRegisterResponse
        """
        return self._stub.RegisterProject(project_register_request, metadata=self._metadata)

    @_handle_rpc_error
    def update_project(self, project):
        """
        Update an existing project specified by id.
        :param flyteidl.admin.project_pb2.Project project:
        :rtype: flyteidl.admin.project_pb2.ProjectUpdateResponse
        """
        return self._stub.UpdateProject(project)

    ####################################################################################################################
    #
    #  Matching Attributes Endpoints
    #
    ####################################################################################################################
    @_handle_rpc_error
    def update_project_domain_attributes(self, project_domain_attributes_update_request):
        """
        This updates the attributes for a project and domain registered with the Flyte Admin Service
        :param flyteidl.admin.ProjectDomainAttributesUpdateRequest project_domain_attributes_update_request:
        :rtype: flyteidl.admin.ProjectDomainAttributesUpdateResponse
        """
        return self._stub.UpdateProjectDomainAttributes(
            project_domain_attributes_update_request, metadata=self._metadata
        )

    @_handle_rpc_error
    def update_workflow_attributes(self, workflow_attributes_update_request):
        """
        This updates the attributes for a project, domain, and workflow registered with the Flyte Admin Service
        :param flyteidl.admin.UpdateWorkflowAttributesRequest workflow_attributes_update_request:
        :rtype: flyteidl.admin.WorkflowAttributesUpdateResponse
        """
        return self._stub.UpdateWorkflowAttributes(workflow_attributes_update_request, metadata=self._metadata)

    @_handle_rpc_error
    def get_project_domain_attributes(self, project_domain_attributes_get_request):
        """
        This fetches the attributes for a project and domain registered with the Flyte Admin Service
        :param flyteidl.admin.ProjectDomainAttributesGetRequest project_domain_attributes_get_request:
        :rtype: flyteidl.admin.ProjectDomainAttributesGetResponse
        """
        return self._stub.GetProjectDomainAttributes(project_domain_attributes_get_request, metadata=self._metadata)

    @_handle_rpc_error
    def get_workflow_attributes(self, workflow_attributes_get_request):
        """
        This fetches the attributes for a project, domain, and workflow registered with the Flyte Admin Service
        :param flyteidl.admin.GetWorkflowAttributesAttributesRequest workflow_attributes_get_request:
        :rtype: flyteidl.admin.WorkflowAttributesGetResponse
        """
        return self._stub.GetWorkflowAttributes(workflow_attributes_get_request, metadata=self._metadata)

    @_handle_rpc_error
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
