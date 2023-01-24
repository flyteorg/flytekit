from random import randint
from time import sleep

from flytekit import FlyteContextManager, StructuredDataset
from flytekit.core import constants
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.base_plugin import (
    RUNNING,
    SUCCEEDED,
    BackendPluginBase,
    BackendPluginRegistry,
    CreateRequest,
    CreateResponse,
    PollRequest,
    PollResponse,
)
from flytekit.extend.backend.utils import get_task_inputs, get_task_template, upload_output_file
from flytekit.models import literals, task
from flytekit.models.types import LiteralType, StructuredDatasetType


# This plugin is used for performance benchmarking
class DummyPlugin(BackendPluginBase):
    def __init__(self):
        super().__init__(task_type="dummy")

    async def initialize(self):
        pass

    async def create(self, create_request: CreateRequest) -> CreateResponse:
        _ = get_task_template(create_request.task_template_path)
        _ = get_task_inputs(create_request.inputs_path)
        sleep(1)

        return CreateResponse(job_id="fake_id")

    async def poll(self, poll_request: PollRequest) -> PollResponse:
        x = randint(0, 100)
        state = RUNNING
        if x < 20:
            ctx = FlyteContextManager.current_context()
            output_file_dict = {
                constants.OUTPUT_FILE_NAME: literals.LiteralMap(
                    {
                        "results": TypeEngine.to_literal(
                            ctx,
                            StructuredDataset(uri="fake_uri"),
                            StructuredDataset,
                            LiteralType(structured_dataset_type=StructuredDatasetType),
                        )
                    }
                )
            }
            upload_output_file(output_file_dict, poll_request.output_prefix)
            state = SUCCEEDED

        return PollResponse(state=state)

    async def terminate(self, job_id):
        sleep(1)


BackendPluginRegistry.register(DummyPlugin())
