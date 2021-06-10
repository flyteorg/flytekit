import datetime
import logging
import typing
from dataclasses import dataclass
from typing import Type

import great_expectations as ge
from dataclasses_json import dataclass_json
from google.protobuf.json_format import MessageToDict
from google.protobuf.struct_pb2 import Struct
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.exceptions import ValidationError

from flytekit import FlyteContext
from flytekit.extend import TypeEngine, TypeTransformer
from flytekit.models import types as _type_models
from flytekit.models.literals import Literal, Scalar
from flytekit.models.types import LiteralType


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
        checkpoint_params: optional SimpleCheckpoint parameters
        batchrequest_config: batchrequest config
        data_context: directory in which GE's configuration resides
    """

    data_source: str
    expectation_suite: str
    data_connector: str
    checkpoint_params: typing.Optional[typing.Dict[str, typing.Union[str, typing.List[str]]]] = None
    batchrequest_config: BatchConfig = None
    data_context: str = "./great_expectations"


def _float_to_int(message: typing.Dict[str, typing.Any]) -> typing.Dict[str, typing.Any]:
    """
    Convert floats to integers after 'Protobuf Struct' is converted to JSON
    JSON automatically converts ints to floats which isn't desirable for GE
    """
    for key, value in message.items():
        if isinstance(value, float):
            if value.is_integer():
                message[key] = int(value)
        elif isinstance(value, dict):
            _float_to_int(value)
    return message


class GEType(object):
    """
    Use this class to send the GEConfig.

    Args:
        config: GE Plugin configuration

    TODO: Connect Data Docs to Flyte Console.
    """

    @classmethod
    def config(cls) -> typing.Type[GEConfig]:
        return GEConfig(data_source="", data_connector="", expectation_suite="")

    def __class_getitem__(cls, config: typing.Type[GEConfig]) -> typing.Any:
        class _GETypeClass(GEType):
            __origin__ = GEType

            @classmethod
            def config(cls) -> typing.Type[GEConfig]:
                return config

        return _GETypeClass


class GETypeTransformer(TypeTransformer[GEType]):
    def __init__(self):
        super().__init__(name="GE Transformer", t=GEType)

    @staticmethod
    def get_config(t: typing.Type[GEType]) -> typing.Type[GEConfig]:
        return t.config()

    def get_literal_type(self, t: Type[str]) -> LiteralType:
        return LiteralType(simple=_type_models.SimpleType.STRUCT, metadata={})

    def to_literal(
        self,
        ctx: FlyteContext,
        python_val: str,
        python_type: Type[GEType],
        expected: LiteralType,
    ) -> Literal:

        if not isinstance(python_val, str):
            raise AssertionError(f"The dataset has to have string data type; given {type(python_val)}")

        if not GETypeTransformer.get_config(python_type):
            raise ValueError("GEConfig is required")

        s = Struct()
        s.update({"dataset": python_val})
        s.update({"config": GETypeTransformer.get_config(python_type).to_dict()})

        return Literal(Scalar(generic=s))

    def to_python_value(
        self,
        ctx: FlyteContext,
        lv: Literal,
        expected_python_type: Type[GEType],
    ) -> GEType:

        if not (lv and lv.scalar and (lv.scalar.generic or lv.scalar.primitive)):
            raise AssertionError("Can only validate a literal value")

        if not (
            (lv.scalar.generic and "config" in lv.scalar.generic) or GETypeTransformer.get_config(expected_python_type)
        ):
            raise ValueError("GEConfig is required")

        primitive_struct = Struct()
        primitive_struct.update(GETypeTransformer.get_config(expected_python_type).to_dict())

        # fetch the configuration
        final_config = lv.scalar.generic["config"] if lv.scalar.generic else primitive_struct
        conf_dict = MessageToDict(final_config)

        # convert floats to ints
        _float_to_int(conf_dict)

        ge_conf = GEConfig(**conf_dict)

        batchrequest_conf = ge_conf.batchrequest_config

        # fetch the data context
        context = ge.data_context.DataContext(ge_conf.data_context)

        # minimalistic batch request
        final_batch_request = {
            "data_asset_name": lv.scalar.generic["dataset"] if lv.scalar.generic else lv.scalar.primitive.string_value,
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
            raise ValidationError(f"Validation failed!\nCOLUMN\t\tFAILED EXPECTATION\n" + result_string)

        logging.info(f"Validation succeeded!")

        return lv.scalar.generic["dataset"] if lv.scalar.generic else lv.scalar.primitive.string_value


TypeEngine.register(GETypeTransformer())
