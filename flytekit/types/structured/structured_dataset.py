from __future__ import annotations

import collections
import importlib
import os
import re
import types
import typing
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, Generator, Optional, Type, Union

import _datetime
import numpy as _np
import pandas
import pandas as pd
import pyarrow
import pyarrow as pa

if importlib.util.find_spec("pyspark") is not None:
    import pyspark
from dataclasses_json import config, dataclass_json
from marshmallow import fields
from typing_extensions import Annotated, TypeAlias, get_args, get_origin

from flytekit.core.context_manager import FlyteContext, FlyteContextManager
from flytekit.core.type_engine import TypeEngine, TypeTransformer
from flytekit.loggers import logger
from flytekit.models import literals
from flytekit.models import types as type_models
from flytekit.models.literals import Literal, Scalar, StructuredDatasetMetadata
from flytekit.models.types import LiteralType, SchemaType, StructuredDatasetType

T = typing.TypeVar("T")  # StructuredDataset type or a dataframe type
DF = typing.TypeVar("DF")  # Dataframe type

# Protocols
BIGQUERY = "bq"
S3 = "s3"
GCS = "gs"
LOCAL = "/"

# For specifying the storage formats of StructuredDatasets. It's just a string, nothing fancy.
StructuredDatasetFormat: TypeAlias = str

# Storage formats
PARQUET: StructuredDatasetFormat = "parquet"


@dataclass_json
@dataclass
class StructuredDataset(object):
    """
    This is the user facing StructuredDataset class. Please don't confuse it with the literals.StructuredDataset
    class (that is just a model, a Python class representation of the protobuf).
    """

    uri: typing.Optional[os.PathLike] = field(default=None, metadata=config(mm_field=fields.String()))
    file_format: typing.Optional[str] = field(default=PARQUET, metadata=config(mm_field=fields.String()))

    DEFAULT_FILE_FORMAT = PARQUET

    @classmethod
    def columns(cls) -> typing.Dict[str, typing.Type]:
        return {}

    @classmethod
    def column_names(cls) -> typing.List[str]:
        return [k for k, v in cls.columns().items()]

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
        return flyte_dataset_transformer.open_as(ctx, self.literal, self._dataframe_type, self.metadata)

    def iter(self) -> Generator[DF, None, None]:
        if self._dataframe_type is None:
            raise ValueError("No dataframe type set. Use open() to set the local dataframe type you want to use.")
        ctx = FlyteContextManager.current_context()
        return flyte_dataset_transformer.iter_as(
            ctx, self.literal, self._dataframe_type, updated_metadata=self.metadata
        )


def extract_cols_and_format(
    t: typing.Any,
) -> typing.Tuple[Type[T], Optional[typing.OrderedDict[str, Type]], Optional[str], Optional[pa.lib.Schema]]:
    """
    Helper function, just used to iterate through Annotations and extract out the following information:
      - base type, if not Annotated, it will just be the type that was passed in.
      - column information, as a collections.OrderedDict,
      - the storage format, as a ``StructuredDatasetFormat`` (str),
      - pa.lib.Schema

    If more than one of any type of thing is found, an error will be raised.
    If no instances of a given type are found, then None will be returned.

    If we add more things, we should put all the returned items in a dataclass instead of just a tuple.

    :param t: The incoming type which may or may not be Annotated
    :return: Tuple representing
        the original type,
        optional OrderedDict of columns,
        optional str for the format,
        optional pyarrow Schema
    """
    fmt = None
    ordered_dict_cols = None
    pa_schema = None
    if get_origin(t) is Annotated:
        base_type, *annotate_args = get_args(t)
        for aa in annotate_args:
            if isinstance(aa, StructuredDatasetFormat):
                if fmt is not None:
                    raise ValueError(f"A format was already specified {fmt}, cannot use {aa}")
                fmt = aa
            elif isinstance(aa, collections.OrderedDict):
                if ordered_dict_cols is not None:
                    raise ValueError(f"Column information was already found {ordered_dict_cols}, cannot use {aa}")
                ordered_dict_cols = aa
            elif isinstance(aa, pyarrow.Schema):
                if pa_schema is not None:
                    raise ValueError(f"Arrow schema was already found {pa_schema}, cannot use {aa}")
                pa_schema = aa
        return base_type, ordered_dict_cols, fmt, pa_schema

    # We return None as the format instead of parquet or something because the transformer engine may find
    # a better default for the given dataframe type.
    return t, ordered_dict_cols, fmt, pa_schema


class StructuredDatasetEncoder(ABC):
    def __init__(self, python_type: Type[T], protocol: str, supported_format: Optional[str] = None):
        """
        Extend this abstract class, implement the encode function, and register your concrete class with the
        StructuredDatasetTransformerEngine class in order for the core flytekit type engine to handle
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
        StructuredDatasetTransformerEngine class in order for the core flytekit type engine to handle
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
        current_task_metadata: StructuredDatasetMetadata,
    ) -> Union[DF, Generator[DF, None, None]]:
        """
        This is code that will be called by the dataset transformer engine to ultimately translate from a Flyte Literal
        value into a Python instance.

        :param ctx: A FlyteContext, useful in accessing the filesystem and other attributes
        :param flyte_value: This will be a Flyte IDL StructuredDataset Literal - do not confuse this with the
          StructuredDataset class defined also in this module.
        :param current_task_metadata: Metadata object containing the type (and columns if any) for the currently
         executing task. This type may have more or less information than the type information bundled inside the incoming flyte_value.
        :return: This function can either return an instance of the dataframe that this decoder handles, or an iterator
          of those dataframes.
        """
        raise NotImplementedError


def protocol_prefix(uri: str) -> str:
    g = re.search(r"([\w]+)://.*", uri)
    if g and g.groups():
        return g.groups()[0]
    return LOCAL


def convert_schema_type_to_structured_dataset_type(
    column_type: int,
) -> int:
    if column_type == SchemaType.SchemaColumn.SchemaColumnType.INTEGER:
        return type_models.SimpleType.INTEGER
    if column_type == SchemaType.SchemaColumn.SchemaColumnType.FLOAT:
        return type_models.SimpleType.FLOAT
    if column_type == SchemaType.SchemaColumn.SchemaColumnType.STRING:
        return type_models.SimpleType.STRING
    if column_type == SchemaType.SchemaColumn.SchemaColumnType.DATETIME:
        return type_models.SimpleType.DATETIME
    if column_type == SchemaType.SchemaColumn.SchemaColumnType.DURATION:
        return type_models.SimpleType.DURATION
    if column_type == SchemaType.SchemaColumn.SchemaColumnType.BOOLEAN:
        return type_models.SimpleType.BOOLEAN
    else:
        raise AssertionError(f"Unrecognized SchemaColumnType: {column_type}")


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

    @staticmethod
    def _finder(handler_map, df_type: Type, protocol: str, format: str):
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

    @classmethod
    def get_encoder(cls, df_type: Type, protocol: str, format: str):
        return cls._finder(StructuredDatasetTransformerEngine.ENCODERS, df_type, protocol, format)

    @classmethod
    def get_decoder(cls, df_type: Type, protocol: str, format: str):
        return cls._finder(StructuredDatasetTransformerEngine.DECODERS, df_type, protocol, format)

    @classmethod
    def _handler_finder(cls, h: Handlers) -> Dict[str, Handlers]:
        # Maybe think about default dict in the future, but is typing as nice?
        if isinstance(h, StructuredDatasetEncoder):
            top_level = cls.ENCODERS
        elif isinstance(h, StructuredDatasetDecoder):
            top_level = cls.DECODERS
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

        # Instances of StructuredDataset opt-in to the ability of being cached.
        self._hash_overridable = True

    @classmethod
    def register(cls, h: Handlers, default_for_type: Optional[bool] = True, override: Optional[bool] = False):
        """
        Call this with any handler to register it with this dataframe meta-transformer

        The string "://" should not be present in any handler's protocol so we don't check for it.
        """
        lowest_level = cls._handler_finder(h)
        if h.supported_format in lowest_level and override is False:
            raise ValueError(f"Already registered a handler for {(h.python_type, h.protocol, h.supported_format)}")
        lowest_level[h.supported_format] = h
        logger.debug(f"Registered {h} as handler for {h.python_type}, protocol {h.protocol}, fmt {h.supported_format}")

        if default_for_type:
            # TODO: Add logging, think about better ux, maybe default False and warn if doesn't exist.
            cls.DEFAULT_FORMATS[h.python_type] = h.supported_format
            cls.DEFAULT_PROTOCOLS[h.python_type] = h.protocol

        # Register with the type engine as well
        # The semantics as of now are such that it doesn't matter which order these transformers are loaded in, as
        # long as the older Pandas/FlyteSchema transformer do not also specify the override
        engine = StructuredDatasetTransformerEngine()
        TypeEngine.register_additional_type(engine, h.python_type, override=True)

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
        # Check first to see if it's even an SD type. For backwards compatibility, we may be getting a FlyteSchema
        python_type, *attrs = extract_cols_and_format(python_type)
        # In case it's a FlyteSchema
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
        if issubclass(python_type, StructuredDataset) and isinstance(python_val, StructuredDataset):
            # There are three cases that we need to take care of here.

            # 1. A task returns a StructuredDataset that was just a passthrough input. If this happens
            # then return the original literals.StructuredDataset without invoking any encoder
            #
            # Ex.
            #   def t1(dataset: Annotated[StructuredDataset, my_cols]) -> Annotated[StructuredDataset, my_cols]:
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
            #   def t2(uri: str) -> Annotated[StructuredDataset, my_cols]
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
                sdt.format or typing.cast(StructuredDataset, python_val).DEFAULT_FILE_FORMAT,
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
        """
        The only tricky thing with converting a Literal (say the output of an earlier task), to a Python value at
        the start of a task execution, is the column subsetting behavior. For example, if you have,

        def t1() -> Annotated[StructuredDataset, kwtypes(col_a=int, col_b=float)]: ...
        def t2(in_a: Annotated[StructuredDataset, kwtypes(col_b=float)]): ...

        where t2(in_a=t1()), when t2 does in_a.open(pd.DataFrame).all(), it should get a DataFrame
        with only one column.

        +-----------------------------+-----------------------------------------+--------------------------------------+
        |                             |          StructuredDatasetType of the incoming Literal                         |
        +-----------------------------+-----------------------------------------+--------------------------------------+
        | StructuredDatasetType       | Has columns defined                     |  [] columns or None                  |
        | of currently running task   |                                         |                                      |
        +=============================+=========================================+======================================+
        |    Has columns              | The StructuredDatasetType passed to the decoder will have the columns          |
        |    defined                  | as defined by the type annotation of the currently running task.               |
        |                             |                                                                                |
        |                             | Decoders **should** then subset the incoming data to the columns requested.    |
        |                             |                                                                                |
        +-----------------------------+-----------------------------------------+--------------------------------------+
        |   [] columns or None        | StructuredDatasetType passed to decoder | StructuredDatasetType passed to the  |
        |                             | will have the columns from the incoming | decoder will have an empty list of   |
        |                             | Literal. This is the scenario where     | columns.                             |
        |                             | the Literal returned by the running     |                                      |
        |                             | task will have more information than    |                                      |
        |                             | the running task's signature.           |                                      |
        +-----------------------------+-----------------------------------------+--------------------------------------+
        """
        # Detect annotations and extract out all the relevant information that the user might supply
        expected_python_type, column_dict, storage_fmt, pa_schema = extract_cols_and_format(expected_python_type)

        # The literal that we get in might be an old FlyteSchema.
        # We'll continue to support this for the time being. There is some duplicated logic here but let's
        # keep it copy/pasted for clarity
        if lv.scalar.schema is not None:
            schema_columns = lv.scalar.schema.type.columns

            # See the repeated logic below for comments
            if column_dict is None or len(column_dict) == 0:
                final_dataset_columns = []
                if schema_columns is not None and schema_columns != []:
                    for c in schema_columns:
                        final_dataset_columns.append(
                            StructuredDatasetType.DatasetColumn(
                                name=c.name,
                                literal_type=LiteralType(
                                    simple=convert_schema_type_to_structured_dataset_type(c.type),
                                ),
                            )
                        )
                # Dataframe will always be serialized to parquet file by FlyteSchema transformer
                new_sdt = StructuredDatasetType(columns=final_dataset_columns, format=PARQUET)
            else:
                final_dataset_columns = self._convert_ordered_dict_of_columns_to_list(column_dict)
                # Dataframe will always be serialized to parquet file by FlyteSchema transformer
                new_sdt = StructuredDatasetType(columns=final_dataset_columns, format=PARQUET)

            metad = literals.StructuredDatasetMetadata(structured_dataset_type=new_sdt)
            sd_literal = literals.StructuredDataset(
                uri=lv.scalar.schema.uri,
                metadata=metad,
            )

            if issubclass(expected_python_type, StructuredDataset):
                sd = StructuredDataset(dataframe=None, metadata=metad)
                sd._literal_sd = sd_literal
                return sd
            else:
                return self.open_as(ctx, sd_literal, expected_python_type, metad)

        # Start handling for StructuredDataset scalars, first look at the columns
        incoming_columns = lv.scalar.structured_dataset.metadata.structured_dataset_type.columns

        # If the incoming literal, also doesn't have columns, then we just have an empty list, so initialize here
        final_dataset_columns = []
        # If the current running task's input does not have columns defined, or has an empty list of columns
        if column_dict is None or len(column_dict) == 0:
            # but if it does, then we just copy it over
            if incoming_columns is not None and incoming_columns != []:
                final_dataset_columns = incoming_columns.copy()
        # If the current running task's input does have columns defined
        else:
            final_dataset_columns = self._convert_ordered_dict_of_columns_to_list(column_dict)

        new_sdt = StructuredDatasetType(
            columns=final_dataset_columns,
            format=lv.scalar.structured_dataset.metadata.structured_dataset_type.format,
            external_schema_type=lv.scalar.structured_dataset.metadata.structured_dataset_type.external_schema_type,
            external_schema_bytes=lv.scalar.structured_dataset.metadata.structured_dataset_type.external_schema_bytes,
        )
        metad = StructuredDatasetMetadata(structured_dataset_type=new_sdt)

        # A StructuredDataset type, for example
        #   t1(input_a: StructuredDataset)  # or
        #   t1(input_a: Annotated[StructuredDataset, my_cols])
        if issubclass(expected_python_type, StructuredDataset):
            sd = expected_python_type(
                dataframe=None,
                # Note here that the type being passed in
                metadata=metad,
            )
            sd._literal_sd = lv.scalar.structured_dataset
            sd.file_format = metad.structured_dataset_type.format
            return sd

        # If the requested type was not a StructuredDataset, then it means it was a plain dataframe type, which means
        # we should do the opening/downloading and whatever else it might entail right now. No iteration option here.
        return self.open_as(ctx, lv.scalar.structured_dataset, df_type=expected_python_type, updated_metadata=metad)

    def to_html(self, ctx: FlyteContext, python_val: typing.Any, expected_python_type: Type[T]) -> str:
        if isinstance(python_val, StructuredDataset):
            if python_val.dataframe is not None:
                df = python_val.dataframe
            else:
                # Here we only render column information by default instead of opening the structured dataset.
                col = typing.cast(StructuredDataset, python_val).columns()
                df = pd.DataFrame(col, ["column type"])
                return df.to_html()
        else:
            df = python_val

        if isinstance(df, pandas.DataFrame):
            return df.describe().to_html()
        elif isinstance(df, pa.Table):
            return df.to_string()
        elif isinstance(df, _np.ndarray):
            return pd.DataFrame(df).describe().to_html()
        elif importlib.util.find_spec("pyspark") is not None and isinstance(df, pyspark.sql.DataFrame):
            return pd.DataFrame(df.schema, columns=["StructField"]).to_html()
        else:
            raise NotImplementedError("Conversion to html string should be implemented")

    def open_as(
        self,
        ctx: FlyteContext,
        sd: literals.StructuredDataset,
        df_type: Type[DF],
        updated_metadata: StructuredDatasetMetadata,
    ) -> DF:
        """
        :param ctx: A FlyteContext, useful in accessing the filesystem and other attributes
        :param sd:
        :param df_type:
        :param updated_metadata: New metadata type, since it might be different from the metadata in the literal.
        :return: dataframe. It could be pandas dataframe or arrow table, etc.
        """
        protocol = protocol_prefix(sd.uri)
        decoder = self.get_decoder(df_type, protocol, sd.metadata.structured_dataset_type.format)
        result = decoder.decode(ctx, sd, updated_metadata)
        if isinstance(result, types.GeneratorType):
            raise ValueError(f"Decoder {decoder} returned iterator {result} but whole value requested from {sd}")
        return result

    def iter_as(
        self,
        ctx: FlyteContext,
        sd: literals.StructuredDataset,
        df_type: Type[DF],
        updated_metadata: StructuredDatasetMetadata,
    ) -> Generator[DF, None, None]:
        protocol = protocol_prefix(sd.uri)
        decoder = self.DECODERS[df_type][protocol][sd.metadata.structured_dataset_type.format]
        result = decoder.decode(ctx, sd, updated_metadata)
        if not isinstance(result, types.GeneratorType):
            raise ValueError(f"Decoder {decoder} didn't return iterator {result} but should have from {sd}")
        return result

    def _get_dataset_column_literal_type(self, t: Type) -> type_models.LiteralType:
        if t in self._SUPPORTED_TYPES:
            return self._SUPPORTED_TYPES[t]
        if hasattr(t, "__origin__") and t.__origin__ == list:
            return type_models.LiteralType(collection_type=self._get_dataset_column_literal_type(t.__args__[0]))
        if hasattr(t, "__origin__") and t.__origin__ == dict:
            return type_models.LiteralType(map_value_type=self._get_dataset_column_literal_type(t.__args__[1]))
        raise AssertionError(f"type {t} is currently not supported by StructuredDataset")

    def _convert_ordered_dict_of_columns_to_list(
        self, column_map: typing.OrderedDict[str, Type]
    ) -> typing.List[StructuredDatasetType.DatasetColumn]:
        converted_cols: typing.List[StructuredDatasetType.DatasetColumn] = []
        if column_map is None or len(column_map) == 0:
            return converted_cols
        for k, v in column_map.items():
            lt = self._get_dataset_column_literal_type(v)
            converted_cols.append(StructuredDatasetType.DatasetColumn(name=k, literal_type=lt))
        return converted_cols

    def _get_dataset_type(self, t: typing.Union[Type[StructuredDataset], typing.Any]) -> StructuredDatasetType:
        original_python_type, column_map, storage_format, pa_schema = extract_cols_and_format(t)

        # Get the column information
        converted_cols = self._convert_ordered_dict_of_columns_to_list(column_map)

        # Get the format
        default_format = (
            original_python_type.DEFAULT_FILE_FORMAT
            if issubclass(original_python_type, StructuredDataset)
            else self.DEFAULT_FORMATS.get(original_python_type, PARQUET)
        )
        fmt = storage_format or default_format

        return StructuredDatasetType(
            columns=converted_cols,
            format=fmt,
            external_schema_type="arrow" if pa_schema else None,
            external_schema_bytes=typing.cast(pa.lib.Schema, pa_schema).to_string().encode() if pa_schema else None,
        )

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


flyte_dataset_transformer = StructuredDatasetTransformerEngine()
TypeEngine.register(flyte_dataset_transformer)
