from __future__ import annotations

import collections
import os
import re
import types
import typing
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, Generator, Optional, Type, Union

from dataclasses_json import config, dataclass_json
from marshmallow import fields

try:
    from typing import Annotated, get_args, get_origin
except ImportError:
    from typing_extensions import Annotated, get_origin, get_args

import _datetime
import numpy as _np
import pyarrow as pa

from flytekit.core.context_manager import FlyteContext, FlyteContextManager
from flytekit.core.type_engine import TypeTransformer
from flytekit.extend import TypeEngine
from flytekit.loggers import logger
from flytekit.models import literals
from flytekit.models import types as type_models
from flytekit.models.literals import Literal, Scalar, StructuredDatasetMetadata
from flytekit.models.types import LiteralType, StructuredDatasetType

T = typing.TypeVar("T")  # StructuredDataset type or a dataframe type
DF = typing.TypeVar("DF")  # Dataframe type

# Protocols
BIGQUERY = "bq"
S3 = "s3"
LOCAL = "/"

# Storage formats
PARQUET = "parquet"


@dataclass_json
@dataclass
class StructuredDataset(object):
    uri: typing.Optional[os.PathLike] = field(default=None, metadata=config(mm_field=fields.String()))
    file_format: typing.Optional[str] = field(default=PARQUET, metadata=config(mm_field=fields.String()))
    """
    This is the user facing StructuredDataset class. Please don't confuse it with the literals.StructuredDataset
    class (that is just a model, a Python class representation of the protobuf).
    """

    FILE_FORMAT = PARQUET

    @classmethod
    def columns(cls) -> typing.Dict[str, typing.Type]:
        return {}

    @classmethod
    def column_names(cls) -> typing.List[str]:
        return [k for k, v in cls.columns().items()]

    def __class_getitem__(cls, args: typing.Union[typing.Dict[str, typing.Type], tuple]) -> Type[StructuredDataset]:
        if args is None:
            return cls

        format = PARQUET
        if isinstance(args, tuple):
            columns = args[0]
            format = args[1]
        else:
            columns = args

        if not isinstance(columns, dict):
            raise AssertionError(
                f"Columns should be specified as an ordered dict "
                f"of column names and their types, received {type(columns)}"
            )

        if not isinstance(format, str):
            raise AssertionError(f"format should be specified as an string, received {type(format)}")

        # If nothing happened, columns and format are the default, then just use the main class
        if len(columns) == 0 and format == PARQUET:
            return cls

        class _TypedStructuredDataset(StructuredDataset):
            # Get the type engine to see this as kind of a generic
            __origin__ = StructuredDataset
            FILE_FORMAT = format

            @classmethod
            def columns(cls) -> typing.Dict[str, typing.Type]:
                return columns

        return _TypedStructuredDataset

    def __init__(
        self,
        dataframe: typing.Optional[typing.Any] = None,
        uri: Optional[str] = None,
        metadata: typing.Optional[literals.StructuredDatasetMetadata] = None,
        **kwargs,
    ):
        self._dataframe = dataframe
        # Make these fields public, so that the dataclass transformer can set a value for it
        # https://github.com/flyteorg/flytekit/blob/bcc8541bd6227b532f8462563fe8aac902242b21/flytekit/core/type_engine.py#L298
        self.uri = uri
        # This is a special attribute that indicates if the data was either downloaded or uploaded
        self._metadata = metadata
        # This is not for users to set, the transformer will set this.
        self._literal_sd: Optional[literals.StructuredDataset] = None
        # Not meant for users to set, will be set by an open() call
        self._dataframe_type = None

    @property
    def dataframe(self) -> Type[typing.Any]:
        return self._dataframe

    @property
    def metadata(self) -> Optional[StructuredDatasetMetadata]:
        return self._metadata

    @property
    def literal(self) -> Optional[literals.StructuredDataset]:
        return self._literal_sd

    def open(self, dataframe_type: Type[DF]):
        self._dataframe_type = dataframe_type
        return self

    def all(self) -> DF:
        if self._dataframe_type is None:
            raise ValueError("No dataframe type set. Use open() to set the local dataframe type you want to use.")
        ctx = FlyteContextManager.current_context()
        return FLYTE_DATASET_TRANSFORMER.open_as(ctx, self.literal, self._dataframe_type)

    def iter(self) -> Generator[DF, None, None]:
        if self._dataframe_type is None:
            raise ValueError("No dataframe type set. Use open() to set the local dataframe type you want to use.")
        ctx = FlyteContextManager.current_context()
        return FLYTE_DATASET_TRANSFORMER.iter_as(ctx, self.literal, self._dataframe_type)


class StructuredDatasetEncoder(ABC):
    def __init__(self, python_type: Type[T], protocol: str, supported_format: Optional[str] = None):
        """
        Extend this abstract class, implement the encode function, and register your concrete class with the
        FLYTE_DATASET_TRANSFORMER defined at this module level in order for the core flytekit type engine to handle
        dataframe libraries. This is the encoding interface, meaning it is used when there is a Python value that the
        flytekit type engine is trying to convert into a Flyte Literal. For the other way, see
        the StructuredDatasetEncoder

        :param python_type: The dataframe class in question that you want to register this encoder with
        :param protocol: A prefix representing the storage driver (e.g. 's3, 'gs', 'bq', etc.). You can use either
          "s3" or "s3://". They are the same since the "://" will just be stripped by the constructor.
        :param supported_format: Arbitrary string representing the format. If not supplied then an empty string
          will be used. An empty string implies that the encoder works with any format. If the format being asked
          for does not exist, the transformer enginer will look for the "" endcoder instead and write a warning.
        """
        self._python_type = python_type
        self._protocol = protocol.replace("://", "")
        self._supported_format = supported_format or ""

    @property
    def python_type(self) -> Type[T]:
        return self._python_type

    @property
    def protocol(self) -> str:
        return self._protocol

    @property
    def supported_format(self) -> str:
        return self._supported_format

    @abstractmethod
    def encode(
        self,
        ctx: FlyteContext,
        structured_dataset: StructuredDataset,
        structured_dataset_type: StructuredDatasetType,
    ) -> literals.StructuredDataset:
        """
        Even if the user code returns a plain dataframe instance, the dataset transformer engine will wrap the
        incoming dataframe with defaults set for that dataframe
        type. This simplifies this function's interface as a lot of data that could be specified by the user using
        the
        # TODO: Do we need to add a flag to indicate if it was wrapped by the transformer or by the user?

        :param ctx:
        :param structured_dataset: This is a StructuredDataset wrapper object. See more info above.
        :param structured_dataset_type: This the StructuredDatasetType, as found in the LiteralType of the interface
          of the task that invoked this encoding call. It is passed along to encoders so that authors of encoders
          can include it in the returned literals.StructuredDataset. See the IDL for more information on why this
          literal in particular carries the type information along with it. If the encoder doesn't supply it, it will
          also be filled in after the encoder runs by the transformer engine.
        :return: This function should return a StructuredDataset literal object. Do not confuse this with the
          StructuredDataset wrapper class used as input to this function - that is the user facing Python class.
          This function needs to return the IDL StructuredDataset.
        """
        raise NotImplementedError


class StructuredDatasetDecoder(ABC):
    def __init__(self, python_type: Type[DF], protocol: str, supported_format: Optional[str] = None):
        """
        Extend this abstract class, implement the decode function, and register your concrete class with the
        FLYTE_DATASET_TRANSFORMER defined at this module level in order for the core flytekit type engine to handle
        dataframe libraries. This is the decoder interface, meaning it is used when there is a Flyte Literal value,
        and we have to get a Python value out of it. For the other way, see the StructuredDatasetEncoder

        :param python_type: The dataframe class in question that you want to register this decoder with
        :param protocol: A prefix representing the storage driver (e.g. 's3, 'gs', 'bq', etc.). You can use either
          "s3" or "s3://". They are the same since the "://" will just be stripped by the constructor.
        :param supported_format: Arbitrary string representing the format. If not supplied then an empty string
          will be used. An empty string implies that the decoder works with any format. If the format being asked
          for does not exist, the transformer enginer will look for the "" decoder instead and write a warning.
        """
        self._python_type = python_type
        self._protocol = protocol.replace("://", "")
        self._supported_format = supported_format or ""

    @property
    def python_type(self) -> Type[DF]:
        return self._python_type

    @property
    def protocol(self) -> str:
        return self._protocol

    @property
    def supported_format(self) -> str:
        return self._supported_format

    @abstractmethod
    def decode(
        self,
        ctx: FlyteContext,
        flyte_value: literals.StructuredDataset,
    ) -> Union[DF, Generator[DF, None, None]]:
        """
        This is code that will be called by the dataset transformer engine to ultimately translate from a Flyte Literal
        value into a Python instance.

        :param ctx:
        :param flyte_value: This will be a Flyte IDL StructuredDataset Literal - do not confuse this with the
          StructuredDataset class defined also in this module.
        :return: This function can either return an instance of the dataframe that this decoder handles, or an iterator
          of those dataframes.
        """
        raise NotImplementedError


def protocol_prefix(uri: str) -> str:
    g = re.search(r"([\w]+)://.*", uri)
    if g and g.groups():
        return g.groups()[0]
    return LOCAL


class StructuredDatasetTransformerEngine(TypeTransformer[StructuredDataset]):
    """
    Think of this transformer as a higher-level meta transformer that is used for all the dataframe types.
    If you are bringing a custom data frame type, or any data frame type, to flytekit, instead of
    registering with the main type engine, you should register with this transformer instead.
    """

    _SUPPORTED_TYPES: typing.Dict[Type, LiteralType] = {
        _np.int32: type_models.LiteralType(simple=type_models.SimpleType.INTEGER),
        _np.int64: type_models.LiteralType(simple=type_models.SimpleType.INTEGER),
        _np.uint32: type_models.LiteralType(simple=type_models.SimpleType.INTEGER),
        _np.uint64: type_models.LiteralType(simple=type_models.SimpleType.INTEGER),
        int: type_models.LiteralType(simple=type_models.SimpleType.INTEGER),
        _np.float32: type_models.LiteralType(simple=type_models.SimpleType.FLOAT),
        _np.float64: type_models.LiteralType(simple=type_models.SimpleType.FLOAT),
        float: type_models.LiteralType(simple=type_models.SimpleType.FLOAT),
        _np.bool_: type_models.LiteralType(simple=type_models.SimpleType.BOOLEAN),  # type: ignore
        bool: type_models.LiteralType(simple=type_models.SimpleType.BOOLEAN),
        _np.datetime64: type_models.LiteralType(simple=type_models.SimpleType.DATETIME),
        _datetime.datetime: type_models.LiteralType(simple=type_models.SimpleType.DATETIME),
        _np.timedelta64: type_models.LiteralType(simple=type_models.SimpleType.DURATION),
        _datetime.timedelta: type_models.LiteralType(simple=type_models.SimpleType.DURATION),
        _np.string_: type_models.LiteralType(simple=type_models.SimpleType.STRING),
        _np.str_: type_models.LiteralType(simple=type_models.SimpleType.STRING),
        _np.object_: type_models.LiteralType(simple=type_models.SimpleType.STRING),
        str: type_models.LiteralType(simple=type_models.SimpleType.STRING),
    }

    ENCODERS: Dict[Type, Dict[str, Dict[str, StructuredDatasetEncoder]]] = {}
    DECODERS: Dict[Type, Dict[str, Dict[str, StructuredDatasetDecoder]]] = {}
    DEFAULT_PROTOCOLS: Dict[Type, str] = {}
    DEFAULT_FORMATS: Dict[Type, str] = {}

    Handlers = Union[StructuredDatasetEncoder, StructuredDatasetDecoder]

    def _finder(self, handler_map, df_type: Type, protocol: str, format: str):
        try:
            return handler_map[df_type][protocol][format]
        except KeyError:
            try:
                hh = handler_map[df_type][protocol][""]
                logger.info(
                    f"Didn't find format specific handler {type(handler_map)} for protocol {protocol}"
                    f" format {format}, using default instead."
                )
                return hh
            except KeyError:
                ...
        raise ValueError(f"Failed to find a handler for {df_type}, protocol {protocol}, fmt {format}")

    def get_encoder(self, df_type: Type, protocol: str, format: str):
        return self._finder(self.ENCODERS, df_type, protocol, format)

    def get_decoder(self, df_type: Type, protocol: str, format: str):
        return self._finder(self.DECODERS, df_type, protocol, format)

    def _handler_finder(self, h: Handlers) -> Dict[str, Handlers]:
        # Maybe think about default dict in the future, but is typing as nice?
        if isinstance(h, StructuredDatasetEncoder):
            top_level = self.ENCODERS
        elif isinstance(h, StructuredDatasetDecoder):
            top_level = self.DECODERS
        else:
            raise TypeError(f"We don't support this type of handler {h}")
        if h.python_type not in top_level:
            top_level[h.python_type] = {}
        if h.protocol not in top_level[h.python_type]:
            top_level[h.python_type][h.protocol] = {}
        return top_level[h.python_type][h.protocol]

    def __init__(self):
        super().__init__("StructuredDataset Transformer", StructuredDataset)
        self._type_assertions_enabled = False

    def register_handler(self, h: Handlers, default_for_type: Optional[bool] = True, override: Optional[bool] = False):
        """
        Call this with any handler to register it with this dataframe meta-transformer

        The string "://" should not be present in any handler's protocol so we don't check for it.
        """
        lowest_level = self._handler_finder(h)
        if h.supported_format in lowest_level and override is False:
            raise ValueError(f"Already registered a handler for {(h.python_type, h.protocol, h.supported_format)}")
        lowest_level[h.supported_format] = h
        logger.debug(f"Registered {h} as handler for {h.python_type}, protocol {h.protocol}, fmt {h.supported_format}")

        if default_for_type:
            # TODO: Add logging, think about better ux, maybe default False and warn if doesn't exist.
            self.DEFAULT_FORMATS[h.python_type] = h.supported_format
            self.DEFAULT_PROTOCOLS[h.python_type] = h.protocol

        # Register with the type engine as well
        # The semantics as of now are such that it doesn't matter which order these transformers are loaded in, as
        # long as the older Pandas/FlyteSchema transformer do not also specify the override
        TypeEngine.register_additional_type(self, h.python_type, override=True)

    def assert_type(self, t: Type[StructuredDataset], v: typing.Any):
        return

    def to_literal(
        self,
        ctx: FlyteContext,
        python_val: Union[StructuredDataset, typing.Any],
        python_type: Union[Type[StructuredDataset], Type],
        expected: LiteralType,
    ) -> Literal:
        # Make a copy in case we need to hand off to encoders, since we can't be sure of mutations.
        # Check first to see if it's even an SD type. For backwards compatibility, we may be getting a
        if get_origin(python_type) is Annotated:
            python_type = get_args(python_type)[0]
        sdt = StructuredDatasetType(format=self.DEFAULT_FORMATS.get(python_type, None))

        if expected and expected.structured_dataset_type:
            sdt = StructuredDatasetType(
                columns=expected.structured_dataset_type.columns,
                format=expected.structured_dataset_type.format,
                external_schema_type=expected.structured_dataset_type.external_schema_type,
                external_schema_bytes=expected.structured_dataset_type.external_schema_bytes,
            )

        # If the type signature has the StructuredDataset class, it will, or at least should, also be a
        # StructuredDataset instance.
        if issubclass(python_type, StructuredDataset):
            assert isinstance(python_val, StructuredDataset)
            # There are three cases that we need to take care of here.

            # 1. A task returns a StructuredDataset that was just a passthrough input. If this happens
            # then return the original literals.StructuredDataset without invoking any encoder
            #
            # Ex.
            #   def t1(dataset: StructuredDataset[my_cols]) -> StructuredDataset[my_cols]:
            #       return dataset
            if python_val._literal_sd is not None:
                if python_val.dataframe is not None:
                    raise ValueError(
                        f"Shouldn't have specified both literal {python_val._literal_sd} and dataframe {python_val.dataframe}"
                    )
                return Literal(scalar=Scalar(structured_dataset=python_val._literal_sd))

            # 2. A task returns a python StructuredDataset with a uri.
            # Note: this case is also what happens we start a local execution of a task with a python StructuredDataset.
            #  It gets converted into a literal first, then back into a python StructuredDataset.
            #
            # Ex.
            #   def t2(uri: str) -> StructuredDataset[my_cols]
            #       return StructuredDataset(uri=uri)
            if python_val.dataframe is None:
                if not python_val.uri:
                    raise ValueError(f"If dataframe is not specified, then the uri should be specified. {python_val}")
                sd_model = literals.StructuredDataset(
                    uri=python_val.uri,
                    metadata=StructuredDatasetMetadata(structured_dataset_type=sdt),
                )
                return Literal(scalar=Scalar(structured_dataset=sd_model))

            # 3. This is the third and probably most common case. The python StructuredDataset object wraps a dataframe
            # that we will need to invoke an encoder for. Figure out which encoder to call and invoke it.
            df_type = type(python_val.dataframe)
            if python_val.uri is None:
                protocol = self.DEFAULT_PROTOCOLS[df_type]
            else:
                protocol = protocol_prefix(python_val.uri)
            return self.encode(
                ctx,
                python_val,
                df_type,
                protocol,
                python_val.file_format,
                sdt,
            )

        # Otherwise assume it's a dataframe instance. Wrap it with some defaults
        fmt = self.DEFAULT_FORMATS[python_type]
        protocol = self.DEFAULT_PROTOCOLS[python_type]
        meta = StructuredDatasetMetadata(structured_dataset_type=expected.structured_dataset_type if expected else None)
        sd = StructuredDataset(dataframe=python_val, metadata=meta)
        return self.encode(ctx, sd, python_type, protocol, fmt, sdt)

    def encode(
        self,
        ctx: FlyteContext,
        sd: StructuredDataset,
        df_type: Type,
        protocol: str,
        format: str,
        structured_literal_type: StructuredDatasetType,
    ) -> Literal:
        handler: StructuredDatasetEncoder
        handler = self.get_encoder(df_type, protocol, format)
        sd_model = handler.encode(ctx, sd, structured_literal_type)
        # This block is here in case the encoder did not set the type information in the metadata. Since this literal
        # is special in that it carries around the type itself, we want to make sure the type info therein is at
        # least as good as the type of the interface.
        if sd_model.metadata is None:
            sd_model._metadata = StructuredDatasetMetadata(structured_literal_type)
        if sd_model.metadata.structured_dataset_type is None:
            sd_model.metadata._structured_dataset_type = structured_literal_type
        # Always set the format here to the format of the handler.
        # Note that this will always be the same as the incoming format except for when the fallback handler
        # with a format of "" is used.
        sd_model.metadata._structured_dataset_type.format = handler.supported_format
        return Literal(scalar=Scalar(structured_dataset=sd_model))

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]) -> T:
        # The literal that we get in might be an old FlyteSchema.
        # We'll continue to support this for the time being.
        if get_origin(expected_python_type) is Annotated:
            expected_python_type = get_args(expected_python_type)[0]
        if lv.scalar.schema is not None:
            sd = StructuredDataset()
            sd_literal = literals.StructuredDataset(
                uri=lv.scalar.schema.uri,
                metadata=literals.StructuredDatasetMetadata(
                    # Dataframe will always be serialized to parquet file by FlyteSchema transformer
                    structured_dataset_type=StructuredDatasetType(format=PARQUET)
                ),
            )
            sd._literal_sd = sd_literal
            if issubclass(expected_python_type, StructuredDataset):
                return sd
            else:
                return self.open_as(ctx, sd_literal, df_type=expected_python_type)

        # Either a StructuredDataset type or some dataframe type.
        if issubclass(expected_python_type, StructuredDataset):
            # Just save the literal for now. If in the future we find that we need the StructuredDataset type hint
            # type also, we can add it.
            sd = expected_python_type(
                dataframe=None,
                # Specifying these two are just done for completeness. Kind of waste since
                # we're saving the whole incoming literal to _literal_sd.
                metadata=lv.scalar.structured_dataset.metadata,
            )
            sd._literal_sd = lv.scalar.structured_dataset
            return sd

        # If the requested type was not a StructuredDataset, then it means it was a plain dataframe type, which means
        # we should do the opening/downloading and whatever else it might entail right now. No iteration option here.
        return self.open_as(ctx, lv.scalar.structured_dataset, df_type=expected_python_type)

    def open_as(self, ctx: FlyteContext, sd: literals.StructuredDataset, df_type: Type[DF]) -> DF:
        protocol = protocol_prefix(sd.uri)
        decoder = self.get_decoder(df_type, protocol, sd.metadata.structured_dataset_type.format)
        result = decoder.decode(ctx, sd)
        if isinstance(result, types.GeneratorType):
            raise ValueError(f"Decoder {decoder} returned iterator {result} but whole value requested from {sd}")
        return result

    def iter_as(
        self, ctx: FlyteContext, sd: literals.StructuredDataset, df_type: Type[DF]
    ) -> Generator[DF, None, None]:
        protocol = protocol_prefix(sd.uri)
        decoder = self.DECODERS[df_type][protocol][sd.metadata.structured_dataset_type.format]
        result = decoder.decode(ctx, sd)
        if not isinstance(result, types.GeneratorType):
            raise ValueError(f"Decoder {decoder} didn't return iterator {result} but should have from {sd}")
        return result

    def _get_dataset_column_literal_type(self, t: Type):
        if t in self._SUPPORTED_TYPES:
            return self._SUPPORTED_TYPES[t]
        if hasattr(t, "__origin__") and t.__origin__ == list:
            return type_models.LiteralType(collection_type=self._get_dataset_column_literal_type(t.__args__[0]))
        if hasattr(t, "__origin__") and t.__origin__ == dict:
            return type_models.LiteralType(map_value_type=self._get_dataset_column_literal_type(t.__args__[1]))
        raise AssertionError(f"type {t} is currently not supported by StructuredDataset")

    def _get_dataset_type(self, t: typing.Union[Type[StructuredDataset], typing.Any]) -> StructuredDatasetType:
        converted_cols: typing.List[StructuredDatasetType.DatasetColumn] = []
        # Handle different kinds of annotation
        # my_cols = kwtypes(x=int, y=str)
        # 1. Fill in format correctly by checking for typing.annotated. For example, Annotated[pd.Dataframe, my_cols]
        if get_origin(t) is Annotated:
            _, *hint_args = get_args(t)
            if type(hint_args[0]) is collections.OrderedDict:
                for k, v in hint_args[0].items():
                    lt = self._get_dataset_column_literal_type(v)
                    converted_cols.append(StructuredDatasetType.DatasetColumn(name=k, literal_type=lt))
                return StructuredDatasetType(columns=converted_cols, format=PARQUET)
            # 3. Fill in external schema type and bytes by checking for typing.annotated metadata.
            # For example, Annotated[pd.Dataframe, pa.schema([("col1", pa.int32()), ("col2", pa.string())])]
            elif type(hint_args[0]) is pa.lib.Schema:
                return StructuredDatasetType(
                    format=PARQUET,
                    external_schema_type="arrow",
                    external_schema_bytes=typing.cast(pa.lib.Schema, hint_args[0]).to_string().encode(),
                )
            raise ValueError(f"Unrecognized Annotated type for StructuredDataset {t}")

        # 2. Fill in columns by checking for StructuredDataset metadata. For example, StructuredDataset[my_cols, parquet]
        elif issubclass(t, StructuredDataset):
            for k, v in t.columns().items():
                lt = self._get_dataset_column_literal_type(v)
                converted_cols.append(StructuredDatasetType.DatasetColumn(name=k, literal_type=lt))
            return StructuredDatasetType(columns=converted_cols, format=t.FILE_FORMAT)

        # 3. pd.Dataframe
        else:
            fmt = self.DEFAULT_FORMATS.get(t, PARQUET)
            return StructuredDatasetType(columns=converted_cols, format=fmt)

    def get_literal_type(self, t: typing.Union[Type[StructuredDataset], typing.Any]) -> LiteralType:
        """
        Provide a concrete implementation so that writers of custom dataframe handlers since there's nothing that
        special about the literal type. Any dataframe type will always be associated with the structured dataset type.
        The other aspects of it - columns, external schema type, etc. can be read from associated metadata.

        :param t: The python dataframe type, which is mostly ignored.
        """
        return LiteralType(structured_dataset_type=self._get_dataset_type(t))

    def guess_python_type(self, literal_type: LiteralType) -> Type[T]:
        # todo: technically we should return the dataframe type specified in the constructor, but to do that,
        #   we'd have to store that, which we don't do today. See possibly #1363
        if literal_type.structured_dataset_type is not None:
            return StructuredDataset
        raise ValueError(f"StructuredDatasetTransformerEngine cannot reverse {literal_type}")


FLYTE_DATASET_TRANSFORMER = StructuredDatasetTransformerEngine()
TypeEngine.register(FLYTE_DATASET_TRANSFORMER)
