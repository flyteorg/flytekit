import datetime
import logging
import typing
from dataclasses import dataclass

import great_expectations as ge
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.exceptions import ValidationError

from flytekit import PythonInstanceTask
from flytekit.extend import Interface

T = typing.TypeVar("T")


@dataclass
class BatchRequestConfig(object):
    """
    Use this configuration to configure Batch Request. A BatchRequest can either be
    a simple BatchRequest or a RuntimeBatchRequest.

    Args:
        data_connector_query: query to request data batch
        runtime_parameters: parameters to be passed at runtime
        batch_identifiers: identifiers to identify the data batch
        batch_spec_passthrough: reader method if your file doesn't have an extension
    """

    data_connector_query: typing.Optional[typing.Dict[str, typing.Any]] = None
    runtime_parameters: typing.Optional[typing.Dict[str, str]] = None
    batch_identifiers: typing.Optional[typing.Dict[str, str]] = None
    batch_spec_passthrough: typing.Optional[typing.Dict[str, typing.Any]] = None


class GETask(PythonInstanceTask[BatchRequestConfig]):
    """
    This task can be used to validate your data.

    Args:
        name: name of the task
        data_source: tell where your data lives and how to get it
        expectation_suite: suite which consists of the data expectations
        data_connector: connector to identify data batches
        inputs: inputs to pass to the execute() method
        checkpoint_params: optional SimpleCheckpoint parameters
        task_config: batchrequest config
        data_context: directory in which GE's configuration resides

    TODO: Connect Data Docs to Flyte Console.
    """

    _TASK_TYPE = "greatexpectations"

    def __init__(
        self,
        name: str,
        data_source: str,
        expectation_suite: str,
        data_connector: str,
        inputs: typing.Dict[str, typing.Type],
        checkpoint_params: typing.Optional[typing.Dict[str, typing.Union[str, typing.List[str]]]] = None,
        task_config: BatchRequestConfig = None,
        data_context: str = "./great_expectations",
        **kwargs,
    ) -> typing.Dict[str, str]:

        self._data_source = data_source
        self._data_connector = data_connector
        self._expectation_suite = expectation_suite
        self._batch_request = task_config
        self._data_context = data_context
        self._checkpoint_params = checkpoint_params

        super(GETask, self).__init__(
            name=name,
            task_config=task_config,
            task_type=self._TASK_TYPE,
            interface=Interface(inputs=inputs),
            **kwargs,
        )

    def execute(self, **kwargs) -> typing.Any:
        context = ge.data_context.DataContext(self._data_context)

        if len(self.python_interface.inputs.keys()) != 1:
            raise RuntimeError(f"Expected one input argument to validate the dataset")

        dataset = kwargs[list(self.python_interface.inputs.keys())[0]]

        if type(dataset) != str:
            raise RuntimeError(f"'dataset' has to have string data type")

        final_batch_request = {
            "data_asset_name": dataset,
            "datasource_name": self._data_source,
            "data_connector_name": self._data_connector,
        }

        # GE's RuntimeBatchRequest
        if self._batch_request and self._batch_request.runtime_parameters:
            final_batch_request.update(
                {
                    "runtime_parameters": self._batch_request.runtime_parameters,
                    "batch_identifiers": self._batch_request.batch_identifiers,
                    "batch_spec_passthrough": self._batch_request.batch_spec_passthrough,
                }
            )

        # GE's BatchRequest
        elif self._batch_request:
            final_batch_request.update(
                {
                    "data_connector_query": self._batch_request.data_connector_query,
                }
            )

        checkpoint_config = {
            "class_name": "SimpleCheckpoint",
            "validations": [
                {
                    "batch_request": final_batch_request,
                    "expectation_suite_name": self._expectation_suite,
                }
            ],
        }

        if self._checkpoint_params:
            checkpoint = SimpleCheckpoint(
                f"_tmp_checkpoint_{self._expectation_suite}", context, **checkpoint_config, **self._checkpoint_params
            )
        else:
            checkpoint = SimpleCheckpoint(f"_tmp_checkpoint_{self._expectation_suite}", context, **checkpoint_config)

        # identify every run uniquely
        run_id = RunIdentifier(
            **{
                "run_name": self._data_source + "_run",
                "run_time": datetime.datetime.utcnow(),
            }
        )

        checkpoint_result = checkpoint.run(run_id=run_id)
        final_result = convert_to_json_serializable(checkpoint_result.list_validation_results())[0]

        result_string = ""
        if final_result["success"] is False:
            for every_result in final_result["results"]:
                if every_result["success"] is False:
                    result_string += (
                        every_result["expectation_config"]["kwargs"]["column"]
                        + " -> "
                        + every_result["expectation_config"]["expectation_type"]
                        + "\n"
                    )

            # raise a GE exception
            raise ValidationError(f"Validation failed!\nCOLUMN\t\tFAILED EXPECTATION\n" + result_string)

        logging.info(f"Validation succeeded!")

        return final_result
