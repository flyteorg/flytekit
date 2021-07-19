import datetime
import logging
import os
import shutil
import typing
from dataclasses import dataclass

import great_expectations as ge
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.exceptions import ValidationError

from flytekit import PythonInstanceTask
from flytekit.core.context_manager import FlyteContext
from flytekit.extend import Interface
from flytekit.types.file.file import FlyteFile
from flytekit.types.schema import FlyteSchema

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
        limit: number of data points to fetch
    """

    data_connector_query: typing.Optional[typing.Dict[str, typing.Any]] = None
    runtime_parameters: typing.Optional[typing.Dict[str, str]] = None
    batch_identifiers: typing.Optional[typing.Dict[str, str]] = None
    batch_spec_passthrough: typing.Optional[typing.Dict[str, typing.Any]] = None
    limit: typing.Optional[int] = None


class GETask(PythonInstanceTask[BatchRequestConfig]):
    """
    This task can be used to validate your data.

    Args:
        name: name of the task
        data_source: tell where your data lives and how to get it
        expectation_suite: suite which consists of the data expectations
        data_connector: connector to identify data batches
        inputs: inputs to pass to the execute() method
        local_file_path: dataset file path useful for FlyteFile and FlyteSchema
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
        local_file_path: str = None,
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
        """
        local_file_path is a must in two scenrios:
        * When using FlyteSchema
        * When using FlyteFile for remote paths
        This is because base directory which has the dataset file 'must' be given in GE's config file
        """
        self._local_file_path = local_file_path
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
            raise RuntimeError("Expected one input argument to validate the dataset")

        dataset = kwargs[list(self.python_interface.inputs.keys())[0]]

        datatype = list(self.python_interface.inputs.values())[0]

        if not issubclass(datatype, (FlyteFile, FlyteSchema, str)):
            raise RuntimeError("'dataset' has to have FlyteFile/FlyteSchema/str datatype")

        # FlyteFile
        if issubclass(datatype, FlyteFile):

            # str
            # if the file is remote, download the file into local_file_path
            if issubclass(type(dataset), str):
                if FlyteContext.current_context().file_access.is_remote(dataset):
                    if not self._local_file_path:
                        raise ValueError("local_file_path is missing!")

                    if os.path.isdir(self._local_file_path):
                        local_path = os.path.join(self._local_file_path, os.path.basename(dataset))
                    else:
                        local_path = self._local_file_path

                    FlyteContext.current_context().file_access.get_data(
                        remote_path=dataset,
                        local_path=local_path,
                    )

            # _SpecificFormatClass
            # if the file is remote, copy the downloaded file to the user specified local_file_path
            else:
                if dataset.remote_source:
                    if not self._local_file_path:
                        raise ValueError("local_file_path is missing!")
                    shutil.copy(dataset, self._local_file_path)

            dataset = os.path.basename(dataset)

        # FlyteSchema
        # convert schema to parquet file
        if issubclass(datatype, FlyteSchema):
            if not self._local_file_path:
                raise ValueError("local_file_path is missing!")

            # FlyteSchema
            if type(dataset) is FlyteSchema:
                # copy parquet file to user-given directory
                FlyteContext.current_context().file_access.download_directory(
                    dataset.remote_path, self._local_file_path
                )

            # DataFrame (Pandas, Spark, etc.)
            else:
                if not os.path.exists(self._local_file_path):
                    os.makedirs(self._local_file_path, exist_ok=True)

                schema = FlyteSchema(
                    local_path=self._local_file_path,
                )
                writer = schema.open(type(dataset))
                writer.write(dataset)
            dataset = os.path.basename(self._local_file_path)

        # minimalistic batch request
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
                    "batch_spec_passthrough": self._batch_request.batch_spec_passthrough,
                    "limit": self._batch_request.limit,
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
            raise ValidationError("Validation failed!\nCOLUMN\t\tFAILED EXPECTATION\n" + result_string)

        logging.info("Validation succeeded!")

        return final_result
