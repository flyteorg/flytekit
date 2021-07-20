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


@dataclass_json
@dataclass
class BatchConfig(object):
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


@dataclass_json
@dataclass
class GEConfig(object):
    """
    Use this configuration to configure GE Plugin.

    Args:
        data_source: tell where your data lives and how to get it
        expectation_suite: suite which consists of the data expectations
        data_connector: connector to identify data batches
        local_file_path: dataset file path useful for FlyteFile and FlyteSchema
        checkpoint_params: optional SimpleCheckpoint parameters
        batchrequest_config: batchrequest config
        data_context: directory in which GE's configuration resides
    """

    data_source: str
    expectation_suite: str
    data_connector: str
    """
    local_file_path is a must in two scenrios:
    * When using FlyteSchema
    * When using FlyteFile for remote paths
    This is because base directory which has the dataset file 'must' be given in GE's config file
    """
    local_file_path: str = None
    checkpoint_params: typing.Optional[typing.Dict[str, typing.Union[str, typing.List[str]]]] = None
    batchrequest_config: BatchConfig = None
    data_context: str = "./great_expectations"


class GEType(object):
    """
    Use this class to send the GEConfig.

    Args:
        config: GE Plugin configuration

    TODO: Connect Data Docs to Flyte Console.
    """

    @classmethod
    def config(cls) -> typing.Tuple[Type, Type[GEConfig]]:
        return (str, GEConfig(data_source="", data_connector="", expectation_suite=""))

    def __class_getitem__(cls, config: typing.Tuple[Type, Type[GEConfig]]) -> typing.Any:
        if not (isinstance(config, tuple) or len(config) != 2):
            raise AssertionError("GEType must have both datatype and GEConfig")

        class _GETypeClass(GEType):
            __origin__ = GEType

            @classmethod
            def config(cls) -> typing.Tuple[Type, Type[GEConfig]]:
                return config

        return _GETypeClass


class GETypeTransformer(TypeTransformer[GEType]):
    def __init__(self):
        super().__init__(name="GE Transformer", t=GEType)

    @staticmethod
    def get_config(t: Type[GEType]) -> typing.Tuple[Type, Type[GEConfig]]:
        return t.config()

    def get_literal_type(self, t: Type[GEType]) -> LiteralType:
        datatype = GETypeTransformer.get_config(t)[0]

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
        python_type: Type[GEType],
        expected: LiteralType,
    ) -> Literal:
        datatype = GETypeTransformer.get_config(python_type)[0]

        if issubclass(datatype, FlyteSchema):
            return FlyteSchemaTransformer().to_literal(ctx, python_val, datatype, expected)
        elif issubclass(datatype, FlyteFile):
            return FlyteFilePathTransformer().to_literal(ctx, python_val, datatype, expected)

        return Literal(scalar=Scalar(primitive=Primitive(string_value=python_val)))

    def to_python_value(
        self,
        ctx: FlyteContext,
        lv: Literal,
        expected_python_type: Type[GEType],
    ) -> GEType:
        if not (lv and lv.scalar and (lv.scalar.primitive or lv.scalar.schema or lv.scalar.blob)):
            raise AssertionError("Can only validate a literal value")

        # fetch the configuration
        conf_dict = GETypeTransformer.get_config(expected_python_type)[1].to_dict()

        ge_conf = GEConfig(**conf_dict)

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

        batchrequest_conf = ge_conf.batchrequest_config

        # fetch the data context
        context = ge.data_context.DataContext(ge_conf.data_context)

        # minimalistic batch request
        final_batch_request = {
            "data_asset_name": dataset,
            "datasource_name": ge_conf.data_source,
            "data_connector_name": ge_conf.data_connector,
        }

        # GE's RuntimeBatchRequest
        if batchrequest_conf and batchrequest_conf["runtime_parameters"]:
            final_batch_request.update(
                {
                    "runtime_parameters": batchrequest_conf["runtime_parameters"],
                    "batch_identifiers": batchrequest_conf["batch_identifiers"],
                    "batch_spec_passthrough": batchrequest_conf["batch_spec_passthrough"],
                }
            )

        # GE's BatchRequest
        elif batchrequest_conf:
            final_batch_request.update(
                {
                    "data_connector_query": batchrequest_conf["data_connector_query"],
                    "batch_spec_passthrough": batchrequest_conf["batch_spec_passthrough"],
                    "limit": batchrequest_conf["limit"],
                }
            )

        checkpoint_config = {
            "class_name": "SimpleCheckpoint",
            "validations": [
                {
                    "batch_request": final_batch_request,
                    "expectation_suite_name": ge_conf.expectation_suite,
                }
            ],
        }

        if ge_conf.checkpoint_params:
            checkpoint = SimpleCheckpoint(
                f"_tmp_checkpoint_{ge_conf.expectation_suite}",
                context,
                **checkpoint_config,
                **ge_conf.checkpoint_params,
            )
        else:
            checkpoint = SimpleCheckpoint(f"_tmp_checkpoint_{ge_conf.expectation_suite}", context, **checkpoint_config)

        # identify every run uniquely
        run_id = RunIdentifier(
            **{
                "run_name": ge_conf.data_source + "_run",
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

        return return_dataset


TypeEngine.register(GETypeTransformer())
