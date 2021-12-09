from __future__ import annotations

import os
import re
import types
import typing
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, Generator, List, Optional, Type, Union

import numpy as _np
import pandas as pd
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


"""

# Python 3.7/3.8
from typing_extensions import Annotated

# Python 3.9+
from typing import Annotated

import pandas as pd

my_pd = Annotated[pd.DataFrame, <stuff>]

Read https://www.python.org/dev/peps/pep-0593/ for more info

@task
def t1() -> my_pd:
    ...

Make the ux for "stuff" something that you like.

Options:
- just add it as a list of tuples:

    Annotated[pd.DataFrame, ('col_a', int), ('col_b', str)]

  This is probably a bad idea - there may be additional data that we want to specify in the future, having one big
  long list is pretty ugly.
  
- Make a class that represents a map of names/types:
    
    class Columns:
       blah: OrderedDict[str, Type]
 
   or something like that. Maybe hook it up to kwtypes
   
     class ColumnStuff:
       @classmethod
       def from_kwtypes(...)
          ...
          
    ColumnStuff.from_kwtypes(kwtypes(cola=int, colb=List[str]))
    
  Obviously think of better names.
  
- Both of these are pretty ugly, so you should think of a better option.

These columns should then make their way into the get_literal_type function (search for the todo "fill in columns by checking for typing.annotated metadata" in this .py file.

One more thing...

We should support manually specified pyarrow schemas:

    import pyarrow as pa
    fields = [
        ('some_int', pa.int32()),
        ('some_string', pa.string()),
    ]
    arr_schema = pa.schema(fields)
    
    Annotated[pd.DataFrame, schema]
    
In this case, we should serialize arr_schema: `arr_schema.serialize().to_pybytes()` and put it into here:
https://github.com/flyteorg/flyteidl/blob/804fa1685264ffded80e3ad1c71ae4c27883187b/protos/flyteidl/core/types.proto#L64
and set the external_schema_type field to "arrow"
    
Let's start out with just the arrow schema. We can add detection of more in the future if people want.


"""


class StructuredDataset(object):
    """
    This is the user facing StructuredDataset class. Please don't confuse it with the literals.StructuredDataset
    class (that is just a model, a Python class representation of the protobuf).
    """

    @classmethod
    def columns(cls) -> typing.Dict[str, typing.Type]:
        return {}

    @classmethod
    def column_names(cls) -> typing.List[str]:
        return [k for k, v in cls.columns().items()]

    def __class_getitem__(cls, columns: typing.Dict[str, typing.Type]) -> Type[StructuredDataset]:
        if columns is None:
            return cls

        if not isinstance(columns, dict):
            raise AssertionError(
                f"Columns should be specified as an ordered dict "
                f"of column names and their types, received {type(columns)}"
            )

        if len(columns) == 0:
            return cls

        class _TypedStructuredDataset(StructuredDataset):
            # Get the type engine to see this as kind of a generic
            __origin__ = StructuredDataset

            @classmethod
            def columns(cls) -> typing.Dict[str, typing.Type]:
                return columns

        return _TypedStructuredDataset

    def __init__(
        self,
        dataframe: typing.Optional[typing.Any] = None,
        uri: Optional[str] = None,
        file_format: str = "parquet",
        metadata: typing.Optional[literals.StructuredDatasetMetadata] = None,
    ):
        self._dataframe = dataframe
        self._uri = uri
        self._file_format = file_format
        # This is a special attribute that indicates if the data was either downloaded or uploaded
        self._metadata = metadata

        # This is not for users to set, the transformer will set this.
        self._literal_sd: Optional[literals.StructuredDataset] = None

    @property
    def dataframe(self) -> Type[typing.Any]:
        return self._dataframe

    @property
    def uri(self) -> Optional[str]:
        return self._uri

    @property
    def file_format(self) -> str:
        return self._file_format

    @property
    def metadata(self) -> Optional[StructuredDatasetMetadata]:
        return self._metadata

    @property
    def literal(self) -> Optional[literals.StructuredDataset]:
        return self._literal_sd

    def open_as(self, df_type: Type[DF]) -> DF:
        ctx = FlyteContextManager.current_context()
        return FLYTE_DATASET_TRANSFORMER.open_as(ctx, self.literal, df_type)

    def iter_as(self, df_type: Type[DF]) -> DF:
        ctx = FlyteContextManager.current_context()
        return FLYTE_DATASET_TRANSFORMER.iter_as(ctx, self.literal, df_type)


class StructuredDatasetEncoder(ABC):
    def __init__(self, python_type: Type[T], protocol: str, supported_format: Optional[str] = None):
        """
        Extend this abstract class, implement the encode function, and register your concrete class with the
        FLYTE_DATASET_TRANSFORMER defined at this module level in order for the core flytekit type engine to handle
        dataframe libraries. This is the encoding interface, meaning it is used when there is a Python value that the
        flytekit type engine is trying to convert into a Flyte Literal. For the other way, see
        the StructuredDatasetEncoder

        :param python_type: The dataframe class in question that you want to register this encoder with
        :param protocol: A prefix representing the storage driver (e.g. 's3, 'gs', 'bq', etc.)
        :param supported_format: Arbitrary string representing the format. If not supplied then an empty string
          will be used. An empty string implies that the encoder works with any format.
        """
        self._python_type = python_type
        self._protocol = protocol
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
    ) -> literals.StructuredDataset:
        """
        Even if the user code returns a plain dataframe instance, the dataset transformer engine will wrap the
        incoming dataframe with defaults set for that dataframe
        type. This simplifies this function's interface as a lot of data that could be specified by the user using
        the
        # TODO: Do we need to add a flag to indicate if it was wrapped by the transformer or by the user?

        :param ctx:
        :param structured_dataset: This is a StructuredDataset wrapper object. See more info above.
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
        :param protocol: A prefix representing the storage driver (e.g. 's3, 'gs', 'bq', etc.)
        :param supported_format: Arbitrary string representing the format. If not supplied then an empty string
          will be used. An empty string implies that the decoder works with any format.
        """
        self._python_type = python_type
        self._protocol = protocol
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
    if g.groups():
        return g.groups()[0]
    raise ValueError(f"No protocol prefix found in {uri}")


class StructuredDatasetTransformerEngine(TypeTransformer[StructuredDataset]):
    """
    Think of this transformer as a higher-level meta transformer that is used for all the dataframe types.
    If you are bringing a custom data frame type, or any data frame type, to flytekit, instead of
    registering with the main type engine, you should register with this transformer instead.
    """

    ENCODERS: Dict[Type, Dict[str, Dict[str, StructuredDatasetEncoder]]] = {}
    DECODERS: Dict[Type, Dict[str, Dict[str, StructuredDatasetDecoder]]] = {}
    DEFAULT_PROTOCOLS: Dict[Type, str] = {}
    DEFAULT_FORMATS: Dict[Type, str] = {}

    Handlers = Union[StructuredDatasetEncoder, StructuredDatasetDecoder]

    def _handler_finder(self, h: Handlers) -> Dict[str, Handlers]:
        # Maybe think about defaultdict in the future, but is typing as nice?
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

    def register_handler(self, h: Handlers, default_for_type: Optional[bool] = True):
        """
        Call this with any handler to register it with this dataframe meta-transformer
        """
        lowest_level = self._handler_finder(h)
        if h.supported_format in lowest_level:
            # TODO: allow overriding
            raise ValueError(f"Already registered a handler for {(h.python_type, h.protocol, h.supported_format)}")
        lowest_level[h.supported_format] = h
        logger.debug(f"Registered {h} as handler for {h.python_type}, protocol {h.protocol}, fmt {h.supported_format}")

        if default_for_type:
            # TODO: Add logging, think about better ux, maybe default False and warn if doesn't exist.
            self.DEFAULT_FORMATS[h.python_type] = h.supported_format
            self.DEFAULT_PROTOCOLS[h.python_type] = h.protocol

        # Register with the type engine as well
        TypeEngine.register_additional_type(self, h.python_type)

    def assert_type(self, t: Type[StructuredDataset], v: typing.Any):
        return

    def to_literal(
        self,
        ctx: FlyteContext,
        python_val: Union[StructuredDataset, typing.Any],
        python_type: Union[Type[StructuredDataset], Type],
        expected: LiteralType,
    ) -> Literal:
        # If the type signature has the StructuredDataset class, it will, or at least should, also be a
        # StructuredDataset instance.
        if issubclass(python_type, StructuredDataset):
            assert isinstance(python_val, StructuredDataset)
            df_type = type(python_val.dataframe)
            format = python_val.file_format
            protocol = self.DEFAULT_PROTOCOLS[df_type]
            try:
                protocol = protocol_prefix(python_val.uri)
            except ValueError:
                ...

            return self.encode(
                ctx,
                python_val,
                df_type,
                protocol,
                format,
            )

        # Otherwise assume it's a dataframe instance. Wrap it with some defaults
        fmt = self.DEFAULT_FORMATS[python_type]
        protocol = self.DEFAULT_PROTOCOLS[python_type]
        meta = StructuredDatasetMetadata(format=fmt, structured_dataset_type=expected.structured_dataset_type)
        sd = StructuredDataset(dataframe=python_val, metadata=meta)
        return self.encode(ctx, sd, python_type, protocol, fmt)

    def encode(self, ctx: FlyteContext, sd: StructuredDataset, df_type: Type, protocol: str, format: str) -> Literal:
        if df_type not in self.ENCODERS:
            raise Exception("not found")

        if protocol not in self.ENCODERS[df_type]:
            raise Exception("not found 2")

        if format not in self.ENCODERS[df_type][protocol]:
            raise Exception("not found 3")

        sd_model = self.ENCODERS[df_type][protocol][format].encode(ctx, sd)
        return Literal(scalar=Scalar(structured_dataset=sd_model))

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]) -> T:
        # Either a StructuredDataset type or some dataframe type.
        if issubclass(expected_python_type, StructuredDataset):
            # Just save the literal for now. If in the future we find that we need the StructuredDataset type hint
            # type also, we can add it.
            sd = expected_python_type(dataframe=None,
                                      # Specifying these two are just done for completeness. Kind of waste since
                                      # we're saving the whole incoming literal to _literal_sd.
                                      file_format=lv.scalar.structured_dataset.metadata.format,
                                      metadata=lv.scalar.structured_dataset.metadata)
            sd._literal_sd = lv.scalar.structured_dataset
            return sd

        # If the requested type was not a StructuredDataset, then it means it was a plain dataframe type, which means
        # we should do the opening/downloading and whatever else it might entail right now. No iteration option here.
        return self.open_as(ctx, lv.scalar.structured_dataset, df_type=expected_python_type)

    def open_as(self, ctx: FlyteContext, sd: literals.StructuredDataset, df_type: Type[DF]) -> DF:
        protocol = protocol_prefix(sd.uri)

        decoder = self.DECODERS[df_type][protocol][sd.metadata.format]
        result = decoder.decode(ctx, sd)
        if isinstance(result, types.GeneratorType):
            raise ValueError(f"Decoder {decoder} returned iterator {result} but whole value requested from {sd}")
        return result

    def iter_as(self, ctx: FlyteContext, sd: literals.StructuredDataset, df_type: Type[DF]) -> DF:
        protocol = protocol_prefix(sd.uri)

        decoder = self.DECODERS[df_type][protocol][sd.metadata.format]
        result = decoder.decode(ctx, sd)
        if not isinstance(result, types.GeneratorType):
            raise ValueError(f"Decoder {decoder} didn't return iterator {result} but should have from {sd}")
        return result

    def get_literal_type(self, t: typing.Union[Type[StructuredDataset], typing.Any]) -> LiteralType:
        """
        Provide a concrete implementation so that writers of custom dataframe handlers since there's nothing that
        special about the literal type. Any dataframe type will always be associated with the structured dataset type.
        The other aspects of it - columns, external schema type, etc. can be read from associated metadata.

        :param t: The python dataframe type, which is mostly ignored.
        """
        # todo: fill in columns by checking for typing.annotated metadata
        # todo: fill in format correctly by checking for typing.annotated metadata, using placeholder for now
        # todo: fill in external schema type and bytes by checking for typing.annotated
        fmt = self.DEFAULT_FORMATS[t] if t in self.DEFAULT_FORMATS else "parquet"
        return LiteralType(structured_dataset_type=StructuredDatasetType(columns=[], format=fmt))

    def guess_python_type(self, literal_type: LiteralType) -> Type[T]:
        # todo: technically we should return the dataframe type specified in the constructor, but to do that,
        #   we'd have to store that, which we don't do today. See possibly #1363
        if literal_type.structured_dataset_type is not None:
            return StructuredDataset


FLYTE_DATASET_TRANSFORMER = StructuredDatasetTransformerEngine()
TypeEngine.register(FLYTE_DATASET_TRANSFORMER)
