import asyncio
import json
import shlex
import subprocess
from dataclasses import asdict, dataclass
from tempfile import NamedTemporaryFile
from typing import Optional

import grpc
from flyteidl.admin.agent_pb2 import CreateTaskResponse, DeleteTaskResponse, GetTaskResponse, Resource
from flytekit import current_context
from flytekit.extend.backend.base_agent import AgentBase, AgentRegistry
from flytekit.loggers import logger
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate

from flytekitplugins.float.utils import async_check_output, float_status_to_flyte_state, flyte_to_float_resources


@dataclass
class Metadata:
    job_id: str


class FloatAgent(AgentBase):
    def __init__(self):
        super().__init__(task_type="float_task")
        self._response_format = ["--format", "json"]

    async def async_create(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
    ) -> CreateTaskResponse:
        """
        Return UID for created task. Return error code if task creation failed.
        """
        secrets = current_context().secrets
        submit_command = [
            "float",
            "submit",
            "--force",
            *self._response_format,
            "--address",
            secrets.get("mmc_address"),
            "--username",
            secrets.get("mmc_username"),
            "--password",
            secrets.get("mmc_password"),
        ]

        container = task_template.container

        min_cpu, min_mem, max_cpu, max_mem = flyte_to_float_resources(container.resources)
        submit_command.extend(["--cpu", f"{min_cpu}:{max_cpu}", "--mem", f"{min_mem}:{max_mem}"])

        image = container.image
        submit_command.extend(["--image", image])

        env = container.env
        for key, value in env.items():
            submit_command.extend(["--env", f"{key}={value}"])

        submit_extra = task_template.custom["submit_extra"]
        submit_command.extend(shlex.split(submit_extra))

        args = task_template.container.args
        script_lines = ["#!/bin/bash\n", f"{shlex.join(args)}\n"]

        task_id = task_template.id
        try:
            with NamedTemporaryFile(mode="w") as job_file:
                job_file.writelines(script_lines)
                job_file.flush()
                logger.debug("Wrote job script")

                submit_command.extend(["--job", job_file.name])

                logger.info(f"Attempting to submit Flyte task {task_id} as float job")
                logger.debug(f"With command: {submit_command}")
                try:
                    submit_response = await async_check_output(*submit_command)
                    submit_response = json.loads(submit_response.decode())
                    job_id = submit_response["id"]
                except subprocess.CalledProcessError as e:
                    logger.exception(
                        f"Failed to submit Flyte task {task_id} as float job\n"
                        f"[stdout] {e.stdout.decode()}\n"
                        f"[stderr] {e.stderr.decode()}\n"
                    )
                    raise
                except (UnicodeError, json.JSONDecodeError):
                    logger.exception(f"Failed to decode submit response for Flyte task: {task_id}")
                    raise
                except KeyError:
                    logger.exception(f"Failed to obtain float job id for Flyte task: {task_id}")
                    raise
        except OSError:
            logger.exception("Cannot open job script for writing")
            raise

        logger.info(f"Submitted Flyte task {task_id} as float job {job_id}")
        logger.debug(f"OpCenter response: {submit_response}")

        metadata = Metadata(job_id=job_id)

        return CreateTaskResponse(resource_meta=json.dumps(asdict(metadata)).encode("utf-8"))

    async def async_get(self, context: grpc.ServicerContext, resource_meta: bytes) -> GetTaskResponse:
        """
        Return the status of the task, and return the outputs on success.
        """
        metadata = Metadata(**json.loads(resource_meta.decode("utf-8")))
        job_id = metadata.job_id

        secrets = current_context().secrets
        show_command = [
            "float",
            "show",
            *self._response_format,
            "--address",
            secrets.get("mmc_address"),
            "--username",
            secrets.get("mmc_username"),
            "--password",
            secrets.get("mmc_password"),
            "--job",
            job_id,
        ]

        logger.info(f"Attempting to obtain status for job {job_id}")
        logger.debug(f"With command: {show_command}")
        try:
            show_response = await async_check_output(*show_command)
            show_response = json.loads(show_response.decode())
            job_status = show_response["status"]
        except subprocess.CalledProcessError as e:
            logger.exception(
                f"Failed to get show response for job: {job_id}\n"
                f"[stdout] {e.stdout.decode()}\n"
                f"[stderr] {e.stderr.decode()}\n"
            )
            raise
        except (UnicodeError, json.JSONDecodeError):
            logger.exception(f"Failed to decode show response for job: {job_id}")
            raise
        except KeyError:
            logger.exception(f"Failed to obtain status for job: {job_id}")
            raise

        task_state = float_status_to_flyte_state(job_status)

        # There are too many status checks to log for normal use
        logger.debug(f"Obtained status for job {job_id}: {job_status}")
        logger.debug(f"OpCenter response: {show_response}")

        return GetTaskResponse(resource=Resource(state=task_state))

    async def async_delete(self, context: grpc.ServicerContext, resource_meta: bytes) -> DeleteTaskResponse:
        """
        Delete the task. This call should be idempotent.
        """
        metadata = Metadata(**json.loads(resource_meta.decode("utf-8")))
        job_id = metadata.job_id

        secrets = current_context().secrets
        cancel_command = [
            "float",
            "cancel",
            "--force",
            "--address",
            secrets.get("mmc_address"),
            "--username",
            secrets.get("mmc_username"),
            "--password",
            secrets.get("mmc_password"),
            "--job",
            job_id,
        ]

        logger.info(f"Attempting to cancel job {job_id}")
        logger.debug(f"With command: {cancel_command}")
        try:
            await async_check_output(*cancel_command)
        except subprocess.CalledProcessError as e:
            logger.exception(
                f"Failed to cancel job: {job_id}\n[stdout] {e.stdout.decode()}\n[stderr] {e.stderr.decode()}\n"
            )
            raise

        logger.info(f"Submitted float cancel for job: {job_id}")

        return DeleteTaskResponse()

    def create(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
    ) -> CreateTaskResponse:
        return asyncio.run(
            self.async_create(
                context=context,
                output_prefix=output_prefix,
                task_template=task_template,
                inputs=inputs,
            )
        )

    def get(self, context: grpc.ServicerContext, resource_meta: bytes) -> GetTaskResponse:
        return asyncio.run(self.async_get(context=context, resource_meta=resource_meta))

    def delete(self, context: grpc.ServicerContext, resource_meta: bytes) -> DeleteTaskResponse:
        return asyncio.run(self.async_delete(context=context, resource_meta=resource_meta))


AgentRegistry.register(FloatAgent())
