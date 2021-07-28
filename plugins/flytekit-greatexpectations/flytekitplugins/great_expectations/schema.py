import datetime
import logging
import os
import typing
from dataclasses import dataclass
from typing import Type

import great_expectations as ge
from dataclasses_json import dataclass_json
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.exceptions import ValidationError

from flytekit import FlyteContext
from flytekit.extend import TypeEngine, TypeTransformer
from flytekit.models import types as _type_models
from flytekit.models.literals import Literal, Primitive, Scalar
from flytekit.models.types import LiteralType
from flytekit.types.file.file import FlyteFile, FlyteFilePathTransformer
from flytekit.types.schema.types import FlyteSchema, FlyteSchemaTransformer, SchemaOpenMode

from .task import BatchRequestConfig


@dataclass_json
@dataclass
class GreatExpectationsFlyteConfig(object):
    """
    Use this configuration to configure GreatExpectations Plugin.

    Args:
        datasource_name: tell where your data lives and how to get it
        expectation_suite_name: suite which consists of the data expectations
        data_connector_name: connector to identify data batches
        local_file_path: dataset file path useful for FlyteFile and FlyteSchema
        checkpoint_params: optional SimpleCheckpoint parameters
        batch_request_config: batchrequest config
        context_root_dir: directory in which GreatExpectations' configuration resides
    """

    datasource_name: str
    expectation_suite_name: str
    data_connector_name: str
    """
    local_file_path is a must in two scenrios:
    * When using FlyteSchema
    * When using FlyteFile for remote paths
    This is because base directory which has the dataset file 'must' be given in GreatExpectations' config file
    """
    local_file_path: str = None
    checkpoint_params: typing.Optional[typing.Dict[str, typing.Union[str, typing.List[str]]]] = None
    batch_request_config: BatchRequestConfig = None
    context_root_dir: str = "./great_expectations"


class GreatExpectationsType(object):
    """
    Use this class to send the GreatExpectationsFlyteConfig.

    Args:
        config: GreatExpectations Plugin configuration

    TODO: Connect Data Docs to Flyte Console.
    """

    @classmethod
    def config(cls) -> typing.Tuple[Type, Type[GreatExpectationsFlyteConfig]]:
        return (
            str,
            GreatExpectationsFlyteConfig(datasource_name="", data_connector_name="", expectation_suite_name=""),
        )

    def __class_getitem__(cls, config: typing.Tuple[Type, Type[GreatExpectationsFlyteConfig]]) -> typing.Any:
        if not (isinstance(config, tuple) or len(config) != 2):
            raise AssertionError("GreatExpectationsType must have both datatype and GreatExpectationsFlyteConfig")

        class _GreatExpectationsTypeClass(GreatExpectationsType):
            __origin__ = GreatExpectationsType

            @classmethod
            def config(cls) -> typing.Tuple[Type, Type[GreatExpectationsFlyteConfig]]:
                return config

        return _GreatExpectationsTypeClass


class GreatExpectationsTypeTransformer(TypeTransformer[GreatExpectationsType]):
    def __init__(self):
        super().__init__(name="GreatExpectations Transformer", t=GreatExpectationsType)

    @staticmethod
    def get_config(t: Type[GreatExpectationsType]) -> typing.Tuple[Type, Type[GreatExpectationsFlyteConfig]]:
        return t.config()

    def get_literal_type(self, t: Type[GreatExpectationsType]) -> LiteralType:
        datatype = GreatExpectationsTypeTransformer.get_config(t)[0]

        if issubclass(datatype, str):
            return LiteralType(simple=_type_models.SimpleType.STRING, metadata={})
        elif issubclass(datatype, FlyteFile):
            return FlyteFilePathTransformer().get_literal_type(datatype)
        elif issubclass(datatype, FlyteSchema):
            return FlyteSchemaTransformer().get_literal_type(datatype)

    def to_literal(
        self,
        ctx: FlyteContext,
        python_val: typing.Union[FlyteFile, FlyteSchema, str],
        python_type: Type[GreatExpectationsType],
        expected: LiteralType,
    ) -> Literal:
        datatype = GreatExpectationsTypeTransformer.get_config(python_type)[0]

        if issubclass(datatype, FlyteSchema):
            return FlyteSchemaTransformer().to_literal(ctx, python_val, datatype, expected)
        elif issubclass(datatype, FlyteFile):
            return FlyteFilePathTransformer().to_literal(ctx, python_val, datatype, expected)

        return Literal(scalar=Scalar(primitive=Primitive(string_value=python_val)))

    def to_python_value(
        self,
        ctx: FlyteContext,
        lv: Literal,
        expected_python_type: Type[GreatExpectationsType],
    ) -> GreatExpectationsType:
        if not (lv and lv.scalar and (lv.scalar.primitive or lv.scalar.schema or lv.scalar.blob)):
            raise AssertionError("Can only validate a literal value")

        # fetch the configuration
        conf_dict = GreatExpectationsTypeTransformer.get_config(expected_python_type)[1].to_dict()

        ge_conf = GreatExpectationsFlyteConfig(**conf_dict)

        # file path for FlyteSchema and FlyteFile
        temp_dataset = ""

        # return value
        return_dataset = ""

        # FlyteSchema
        if lv.scalar.schema:
            if not ge_conf.local_file_path:
                raise ValueError("local_file_path is missing!")

            # copy parquet file to user-given directory
            ctx.file_access.download_directory(lv.scalar.schema.uri, ge_conf.local_file_path)

            temp_dataset = os.path.basename(ge_conf.local_file_path)

            def downloader(x, y):
                ctx.file_access.download_directory(x, y)

            return_dataset = (
                FlyteSchema(
                    local_path=ctx.file_access.get_random_local_directory(),
                    remote_path=lv.scalar.schema.uri,
                    downloader=downloader,
                    supported_mode=SchemaOpenMode.READ,
                )
                .open()
                .all()
            )

        # FlyteFile
        if lv.scalar.blob:
            uri = lv.scalar.blob.uri

            # check if the file is remote
            if ctx.file_access.is_remote(uri):
                if not ge_conf.local_file_path:
                    raise ValueError("local_file_path is missing!")

                if os.path.isdir(ge_conf.local_file_path):
                    local_path = os.path.join(ge_conf.local_file_path, os.path.basename(uri))
                else:
                    local_path = ge_conf.local_file_path

                # download the file into local_file_path
                ctx.file_access.get_data(
                    remote_path=uri,
                    local_path=local_path,
                )

            temp_dataset = os.path.basename(uri)

            return_dataset = FlyteFile(uri)

        if lv.scalar.primitive:
            dataset = return_dataset = lv.scalar.primitive.string_value
        else:
            dataset = temp_dataset

        batch_request_conf = ge_conf.batch_request_config

        # fetch the data context
        context = ge.data_context.DataContext(ge_conf.context_root_dir)

        # minimalistic batch request
        final_batch_request = {
            "data_asset_name": dataset,
            "datasource_name": ge_conf.datasource_name,
            "data_connector_name": ge_conf.data_connector_name,
        }

        # Great Expectations' RuntimeBatchRequest
        if batch_request_conf and batch_request_conf["runtime_parameters"]:
            final_batch_request.update(
                {
                    "runtime_parameters": batch_request_conf["runtime_parameters"],
                    "batch_identifiers": batch_request_conf["batch_identifiers"],
                    "batch_spec_passthrough": batch_request_conf["batch_spec_passthrough"],
                }
            )

        # Great Expectations' BatchRequest
        elif batch_request_conf:
            final_batch_request.update(
                {
                    "data_connector_query": batch_request_conf["data_connector_query"],
                    "batch_spec_passthrough": batch_request_conf["batch_spec_passthrough"],
                }
            )

        checkpoint_config = {
            "class_name": "SimpleCheckpoint",
            "validations": [
                {
                    "batch_request": final_batch_request,
                    "expectation_suite_name": ge_conf.expectation_suite_name,
                }
            ],
        }

        if ge_conf.checkpoint_params:
            checkpoint = SimpleCheckpoint(
                f"_tmp_checkpoint_{ge_conf.expectation_suite_name}",
                context,
                **checkpoint_config,
                **ge_conf.checkpoint_params,
            )
        else:
            checkpoint = SimpleCheckpoint(
                f"_tmp_checkpoint_{ge_conf.expectation_suite_name}", context, **checkpoint_config
            )

        # identify every run uniquely
        run_id = RunIdentifier(
            **{
                "run_name": ge_conf.datasource_name + "_run",
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

        return return_dataset


TypeEngine.register(GreatExpectationsTypeTransformer())
