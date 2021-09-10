import datetime
import logging
import os
import shutil
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Type, Union

import great_expectations as ge
from dataclasses_json import dataclass_json
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.exceptions import ValidationError

from flytekit import PythonInstanceTask
from flytekit.core.context_manager import FlyteContext
from flytekit.extend import Interface
from flytekit.types.file.file import FlyteFile
from flytekit.types.schema import FlyteSchema


@dataclass_json
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

    data_connector_query: Optional[Dict[str, Any]] = None
    runtime_parameters: Optional[Dict[str, Any]] = None
    batch_identifiers: Optional[Dict[str, str]] = None
    batch_spec_passthrough: Optional[Dict[str, Any]] = None


class GreatExpectationsTask(PythonInstanceTask[BatchRequestConfig]):
    """
    This task can be used to validate your data.
    You can use this when you want to validate your data within the task or workflow.
    If you want to validate your data as and when the type is given, use the `GreatExpectationsType`.

    Args:
        name: name of the task
        datasource_name: tell where your data lives and how to get it
        expectation_suite_name: suite which consists of the data expectations
        data_connector_name: connector to identify data batches
        inputs: inputs to pass to the execute() method
        data_asset_name: name of the data asset (to be used for RuntimeBatchRequest)
        local_file_path: dataset file path useful for FlyteFile and FlyteSchema
        checkpoint_params: optional SimpleCheckpoint parameters
        task_config: batchrequest config
        context_root_dir: directory in which GreatExpectations' configuration resides

    TODO: Connect Data Docs to Flyte Console.
    """

    _TASK_TYPE = "great_expectations"
    _RUNTIME_VAR_NAME = "runtime"

    def __init__(
        self,
        name: str,
        datasource_name: str,
        expectation_suite_name: str,
        data_connector_name: str,
        inputs: Dict[str, Type],
        data_asset_name: Optional[str] = None,
        outputs: Optional[Dict[str, Type]] = None,
        local_file_path: Optional[str] = None,
        checkpoint_params: Optional[Dict[str, Union[str, List[str]]]] = None,
        task_config: BatchRequestConfig = None,
        context_root_dir: str = "./great_expectations",
        **kwargs,
    ):
        self._datasource_name = datasource_name
        self._data_connector_name = data_connector_name
        self._expectation_suite_name = expectation_suite_name
        self._batch_request_config = task_config
        self._context_root_dir = context_root_dir
        self._data_asset_name = data_asset_name
        """
        local_file_path is a must in two scenarios:
        * When using FlyteSchema
        * When using FlyteFile for remote paths
        This is because base directory which has the dataset file 'must' be given in GreatExpectations' config file
        """
        self._local_file_path = local_file_path
        self._checkpoint_params = checkpoint_params

        outputs = {"result": Dict[Any, Any]}

        super(GreatExpectationsTask, self).__init__(
            name=name,
            task_config=task_config,
            task_type=self._TASK_TYPE,
            interface=Interface(inputs=inputs, outputs=outputs),
            **kwargs,
        )

    def _flyte_file(self, dataset) -> str:
        if not self._local_file_path:
            raise ValueError("local_file_path is missing!")

        # str and remote
        if issubclass(type(dataset), str) and FlyteContext.current_context().file_access.is_remote(dataset):
            # download the file into local_file_path
            if os.path.isdir(self._local_file_path):
                local_path = os.path.join(self._local_file_path, os.path.basename(dataset))
            else:
                local_path = self._local_file_path

            FlyteContext.current_context().file_access.get_data(
                remote_path=dataset,
                local_path=local_path,
            )
        # _SpecificFormatClass
        elif not issubclass(type(dataset), str) and dataset.remote_source:
            shutil.copy(dataset, self._local_file_path)
        else:
            raise ValueError("Local FlyteFiles are not supported; use the string datatype instead")

        dataset = os.path.basename(dataset)

        return dataset

    def _flyte_schema(self, dataset) -> str:
        if not self._local_file_path:
            raise ValueError("local_file_path is missing!")

        # FlyteSchema
        if type(dataset) is FlyteSchema:
            # copy parquet file to user-given directory
            FlyteContext.current_context().file_access.get_data(
                dataset.remote_path, self._local_file_path, is_multipart=True
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

        return dataset

    def execute(self, **kwargs) -> Any:
        context = ge.data_context.DataContext(self._context_root_dir)  # type: ignore

        if len(self.python_interface.inputs.keys()) != 1:
            raise TypeError("Expected one input argument to validate the dataset")

        dataset_key = list(self.python_interface.inputs.keys())[0]
        dataset = kwargs[dataset_key]
        datatype = self.python_interface.inputs[dataset_key]

        if not issubclass(datatype, (FlyteFile, FlyteSchema, str)):
            raise TypeError("'dataset' has to have FlyteFile/FlyteSchema/str datatype")

        # determine the type of data connector
        selected_datasource = list(filter(lambda x: x["name"] == self._datasource_name, context.list_datasources()))

        if not selected_datasource:
            raise ValueError("Datasource doesn't exist!")

        data_connector_class_lookup = {
            data_connector_name: data_connector_class["class_name"]
            for data_connector_name, data_connector_class in selected_datasource[0]["data_connectors"].items()
        }

        specified_data_connector_class = data_connector_class_lookup[self._data_connector_name]

        is_runtime = False
        if specified_data_connector_class == "RuntimeDataConnector":
            is_runtime = True
            if not self._data_asset_name:
                raise ValueError("data_asset_name has to be given in a RuntimeBatchRequest")

        # FlyteFile
        if issubclass(datatype, FlyteFile):
            dataset = self._flyte_file(dataset)

        # FlyteSchema
        # convert schema to parquet file
        if issubclass(datatype, FlyteSchema) and not is_runtime:
            dataset = self._flyte_schema(dataset)

        # minimalistic batch request
        final_batch_request = {
            "data_asset_name": self._data_asset_name if is_runtime else dataset,
            "datasource_name": self._datasource_name,
            "data_connector_name": self._data_connector_name,
        }

        # Great Expectations' RuntimeBatchRequest
        if self._batch_request_config and (self._batch_request_config.runtime_parameters or is_runtime):
            final_batch_request.update(
                {
                    "runtime_parameters": self._batch_request_config.runtime_parameters
                    if self._batch_request_config.runtime_parameters
                    else {},
                    "batch_identifiers": self._batch_request_config.batch_identifiers,
                    "batch_spec_passthrough": self._batch_request_config.batch_spec_passthrough,
                }
            )

            if is_runtime and issubclass(datatype, str):
                final_batch_request["runtime_parameters"]["query"] = dataset
            elif is_runtime and issubclass(datatype, FlyteSchema):
                final_batch_request["runtime_parameters"]["batch_data"] = dataset.open().all()
            else:
                raise AssertionError("Can only use runtime_parameters for query(str)/schema data")

        # Great Expectations' BatchRequest
        elif self._batch_request_config:
            final_batch_request.update(
                {
                    "data_connector_query": self._batch_request_config.data_connector_query,
                    "batch_spec_passthrough": self._batch_request_config.batch_spec_passthrough,
                }
            )

        checkpoint_config = {
            "class_name": "SimpleCheckpoint",
            "validations": [
                {
                    "batch_request": final_batch_request,
                    "expectation_suite_name": self._expectation_suite_name,
                }
            ],
        }

        if self._checkpoint_params:
            checkpoint = SimpleCheckpoint(
                f"_tmp_checkpoint_{self._expectation_suite_name}",
                context,
                **checkpoint_config,
                **self._checkpoint_params,
            )
        else:
            checkpoint = SimpleCheckpoint(
                f"_tmp_checkpoint_{self._expectation_suite_name}", context, **checkpoint_config
            )

        # identify every run uniquely
        run_id = RunIdentifier(
            **{
                "run_name": self._datasource_name + "_run",
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

            # raise a Great Expectations' exception
            raise ValidationError("Validation failed!\nCOLUMN\t\tFAILED EXPECTATION\n" + result_string)

        logging.info("Validation succeeded!")

        return final_result
