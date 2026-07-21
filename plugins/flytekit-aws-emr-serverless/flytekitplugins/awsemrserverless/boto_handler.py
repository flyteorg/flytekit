"""
Boto3 helper for EMR Serverless API operations.

Provides a thin async wrapper around the boto3 EMR Serverless client, with
application-lifecycle management used by the connector.

All outbound boto3 traffic is funneled through two private helpers so that
tests can mock the entire handler at a single boundary (matches the
flytekit-aws-sagemaker ``Boto3ConnectorMixin._call`` pattern):

* :py:meth:`EMRServerlessHandler._call` -- single-shot API calls
* :py:meth:`EMRServerlessHandler._paginate` -- paginated list operations
"""

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

_BOTO_RETRY_CONFIG = BotoConfig(retries={"max_attempts": 3, "mode": "adaptive"})

_APP_STARTED = "STARTED"
_APP_TERMINAL = {"TERMINATED"}
_APP_NEEDS_START = {"CREATED", "STOPPED"}
_APP_TRANSITIONING = {"CREATING", "STARTING"}

_JOB_TERMINAL = {"SUCCESS", "FAILED", "CANCELLED"}


class EMRServerlessHandler:
    """
    Async-friendly wrapper around the boto3 EMR Serverless client.

    Retries are handled by botocore's built-in adaptive retry mode.
    Blocking boto3 calls are offloaded to a thread-pool executor so they
    do not block the asyncio event loop.
    """

    def __init__(self, region: Optional[str] = None):
        self.region = region
        self._client: Optional[Any] = None
        logger.debug("EMRServerlessHandler initialized: region=%s", region or "(default)")

    @property
    def client(self) -> Any:
        if self._client is None:
            kwargs: Dict[str, Any] = {
                "service_name": "emr-serverless",
                "config": _BOTO_RETRY_CONFIG,
            }
            if self.region:
                kwargs["region_name"] = self.region
            self._client = boto3.client(**kwargs)
            logger.debug(
                "Boto3 EMR Serverless client created: region=%s",
                self._client.meta.region_name,
            )
        return self._client

    async def _call(self, method: str, **params: Any) -> Dict[str, Any]:
        """Invoke a boto3 EMR Serverless client method in the thread-pool executor.

        This is the single chokepoint for all non-paginated boto3 API calls
        (``method`` is the boto3 client method name and ``params`` are keyword
        arguments forwarded to the call in the camelCase form the AWS SDK
        expects). Tests can mock the entire handler surface by patching just
        this method. Returns the raw response dict from the boto3 call.
        """
        func = getattr(self.client, method)
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: func(**params))

    async def _paginate(self, method: str, result_key: str, **params: Any) -> List[Dict[str, Any]]:
        """Exhaust a boto3 paginator and return the concatenated items.

        Used for list operations (e.g. ``list_applications``) that may span
        multiple pages. ``method`` is the boto3 client method to paginate,
        ``result_key`` is the key under which each page stores items
        (e.g. ``"applications"`` for ``list_applications``), and ``params``
        are keyword arguments forwarded to ``paginator.paginate()``. Tests
        can mock this independently from ``_call``. Returns a flat list of
        items across all pages.
        """

        def _collect() -> List[Dict[str, Any]]:
            paginator = self.client.get_paginator(method)
            items: List[Dict[str, Any]] = []
            for page in paginator.paginate(**params):
                items.extend(page.get(result_key, []))
            return items

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _collect)

    # ------------------------------------------------------------------
    # Application management
    # ------------------------------------------------------------------

    async def get_application(self, application_id: str) -> Dict[str, Any]:
        logger.debug("GetApplication: applicationId=%s", application_id)
        resp = await self._call("get_application", applicationId=application_id)
        app = resp.get("application", {})
        logger.debug(
            "GetApplication response: id=%s, name=%s, state=%s",
            app.get("applicationId"),
            app.get("name"),
            app.get("state"),
        )
        return app

    async def find_application_by_name(self, name: str) -> Optional[str]:
        """Find an active application ID by name. Returns None if not found."""
        logger.info("Searching for application by name: '%s'", name)
        apps = await self._paginate(
            "list_applications",
            result_key="applications",
            states=["CREATED", "STARTED", "STOPPED"],
        )
        for app in apps:
            if app.get("name") == name:
                logger.info("Found application '%s' with ID: %s", name, app["id"])
                return app["id"]
        logger.info("Application '%s' not found after searching %d app(s)", name, len(apps))
        return None

    async def create_application(
        self,
        name: str,
        release_label: str,
        application_type: str,
        initial_capacity: Optional[Dict[str, Any]] = None,
        maximum_capacity: Optional[Dict[str, Any]] = None,
        network_configuration: Optional[Dict[str, Any]] = None,
        image_configuration: Optional[Dict[str, Any]] = None,
        tags: Optional[Dict[str, str]] = None,
        architecture: Optional[str] = None,
        runtime_configuration: Optional[list] = None,
        scheduler_configuration: Optional[Dict[str, Any]] = None,
        auto_stop_config: Optional[Dict[str, Any]] = None,
    ) -> str:
        logger.info(
            "CreateApplication: name=%s, release_label=%s, type=%s, architecture=%s",
            name,
            release_label,
            application_type,
            architecture,
        )
        params: Dict[str, Any] = {
            "name": name,
            "releaseLabel": release_label,
            "type": application_type,
        }
        if initial_capacity:
            params["initialCapacity"] = initial_capacity
            logger.debug("CreateApplication: initialCapacity provided")
        if maximum_capacity:
            params["maximumCapacity"] = maximum_capacity
            logger.debug("CreateApplication: maximumCapacity provided")
        if network_configuration:
            params["networkConfiguration"] = network_configuration
            logger.debug("CreateApplication: networkConfiguration provided")
        if image_configuration:
            params["imageConfiguration"] = image_configuration
            logger.debug("CreateApplication: imageConfiguration=%s", image_configuration)
        if tags:
            params["tags"] = tags
            logger.debug("CreateApplication: tags=%s", tags)
        if architecture:
            params["architecture"] = architecture
        if runtime_configuration:
            params["runtimeConfiguration"] = runtime_configuration
            logger.debug("CreateApplication: runtimeConfiguration provided")
        if scheduler_configuration:
            params["schedulerConfiguration"] = scheduler_configuration
            logger.debug("CreateApplication: schedulerConfiguration=%s", scheduler_configuration)
        if auto_stop_config:
            params["autoStopConfiguration"] = auto_stop_config
            logger.debug("CreateApplication: autoStopConfiguration=%s", auto_stop_config)

        resp = await self._call("create_application", **params)
        app_id = resp.get("applicationId", "")
        logger.info("CreateApplication succeeded: applicationId=%s, arn=%s", app_id, resp.get("arn", ""))
        return app_id

    async def update_application(
        self,
        application_id: str,
        image_configuration: Optional[Dict[str, Any]] = None,
        maximum_capacity: Optional[Dict[str, Any]] = None,
        auto_stop_config: Optional[Dict[str, Any]] = None,
        runtime_configuration: Optional[list] = None,
        scheduler_configuration: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Update a mutable subset of application properties."""
        params: Dict[str, Any] = {"applicationId": application_id}
        update_fields = []

        if image_configuration:
            params["imageConfiguration"] = image_configuration
            update_fields.append("imageConfiguration")
        if maximum_capacity:
            params["maximumCapacity"] = maximum_capacity
            update_fields.append("maximumCapacity")
        if auto_stop_config:
            params["autoStopConfiguration"] = auto_stop_config
            update_fields.append("autoStopConfiguration")
        if runtime_configuration:
            params["runtimeConfiguration"] = runtime_configuration
            update_fields.append("runtimeConfiguration")
        if scheduler_configuration:
            params["schedulerConfiguration"] = scheduler_configuration
            update_fields.append("schedulerConfiguration")

        if len(params) <= 1:
            logger.debug("UpdateApplication: no fields to update for %s, skipping", application_id)
            return

        logger.info("UpdateApplication: applicationId=%s, fields=%s", application_id, update_fields)
        await self._call("update_application", **params)
        logger.info("UpdateApplication succeeded for %s", application_id)

    async def ensure_application_started(
        self,
        application_id: str,
        timeout_seconds: int = 300,
        poll_interval_seconds: int = 10,
    ) -> None:
        """Ensure the application reaches STARTED state, starting it if needed."""
        logger.info(
            "EnsureApplicationStarted: applicationId=%s, timeout=%ds",
            application_id,
            timeout_seconds,
        )
        start_time = time.monotonic()

        while True:
            app = await self.get_application(application_id)
            state = app.get("state", "")

            if state == _APP_STARTED:
                elapsed = time.monotonic() - start_time
                logger.info(
                    "Application %s is in STARTED state (waited %.1fs)",
                    application_id,
                    elapsed,
                )
                return

            if state in _APP_TERMINAL:
                logger.error(
                    "Application %s is in terminal state '%s', cannot be started",
                    application_id,
                    state,
                )
                raise RuntimeError(f"Application {application_id} is in terminal state '{state}' and cannot be started")

            if state in _APP_NEEDS_START:
                logger.info("Application %s is in state '%s', sending StartApplication request", application_id, state)
                await self._call("start_application", applicationId=application_id)

            elapsed = time.monotonic() - start_time
            if elapsed >= timeout_seconds:
                logger.error(
                    "Timed out after %.1fs waiting for application %s to start (last state: %s)",
                    elapsed,
                    application_id,
                    state,
                )
                raise TimeoutError(
                    f"Timed out after {timeout_seconds}s waiting for application {application_id} to start "
                    f"(last state: {state})"
                )

            logger.debug(
                "Application %s state: %s, waiting %ds (elapsed: %.1fs / %ds)",
                application_id,
                state,
                poll_interval_seconds,
                elapsed,
                timeout_seconds,
            )
            await asyncio.sleep(poll_interval_seconds)

    # ------------------------------------------------------------------
    # Job management
    # ------------------------------------------------------------------

    async def start_job_run(
        self,
        application_id: str,
        execution_role_arn: str,
        job_driver: Dict[str, Any],
        configuration_overrides: Optional[Dict[str, Any]] = None,
        tags: Optional[Dict[str, str]] = None,
        execution_timeout_minutes: int = 60,
        name: Optional[str] = None,
        retry_policy: Optional[Dict[str, Any]] = None,
    ) -> str:
        logger.info(
            "StartJobRun: applicationId=%s, name=%s, timeout=%dm",
            application_id,
            name,
            execution_timeout_minutes,
        )
        params: Dict[str, Any] = {
            "applicationId": application_id,
            "executionRoleArn": execution_role_arn,
            "jobDriver": job_driver,
            "executionTimeoutMinutes": execution_timeout_minutes,
        }
        if configuration_overrides:
            params["configurationOverrides"] = configuration_overrides
            logger.debug("StartJobRun: configurationOverrides provided")
        if tags:
            params["tags"] = tags
        if name:
            params["name"] = name
        if retry_policy:
            params["retryPolicy"] = retry_policy
            logger.debug("StartJobRun: retryPolicy=%s", retry_policy)

        logger.debug("StartJobRun: jobDriver type=%s", list(job_driver.keys()))
        resp = await self._call("start_job_run", **params)
        job_run_id = resp.get("jobRunId", "")
        logger.info(
            "StartJobRun succeeded: jobRunId=%s, applicationId=%s, arn=%s",
            job_run_id,
            application_id,
            resp.get("arn", ""),
        )
        return job_run_id

    async def get_job_run(self, application_id: str, job_run_id: str) -> Dict[str, Any]:
        logger.debug("GetJobRun: applicationId=%s, jobRunId=%s", application_id, job_run_id)
        resp = await self._call(
            "get_job_run",
            applicationId=application_id,
            jobRunId=job_run_id,
        )
        job = resp.get("jobRun", {})
        logger.debug(
            "GetJobRun response: jobRunId=%s, state=%s",
            job.get("jobRunId"),
            job.get("state"),
        )
        return job

    async def cancel_job_run(self, application_id: str, job_run_id: str) -> None:
        """Cancel a running job. Idempotent -- safe to call on completed jobs."""
        logger.info("CancelJobRun: applicationId=%s, jobRunId=%s", application_id, job_run_id)
        try:
            job = await self.get_job_run(application_id, job_run_id)
            current_state = job.get("state", "")
            if current_state in _JOB_TERMINAL:
                logger.info(
                    "CancelJobRun: job %s already in terminal state '%s', no action needed",
                    job_run_id,
                    current_state,
                )
                return
            logger.info("CancelJobRun: job %s is in state '%s', sending cancel request", job_run_id, current_state)
            await self._call(
                "cancel_job_run",
                applicationId=application_id,
                jobRunId=job_run_id,
            )
            logger.info("CancelJobRun succeeded: jobRunId=%s", job_run_id)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code == "ResourceNotFoundException":
                logger.warning(
                    "CancelJobRun: job %s not found (ResourceNotFoundException), may already be cleaned up",
                    job_run_id,
                )
                return
            if code == "ValidationException" and "cannot be cancelled" in str(e).lower():
                logger.info(
                    "CancelJobRun: job %s cannot be cancelled (ValidationException), likely already completed",
                    job_run_id,
                )
                return
            logger.error("CancelJobRun failed for job %s: %s (code: %s)", job_run_id, e, code)
            raise
