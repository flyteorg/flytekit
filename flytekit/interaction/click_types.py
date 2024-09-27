import dataclasses
import datetime
import enum
import json
import logging
import os
import pathlib
import typing
from typing import cast, get_args

import rich_click as click
import yaml
from dataclasses_json import DataClassJsonMixin, dataclass_json
from pytimeparse import parse

from flytekit import BlobType, FlyteContext, Literal, LiteralType, StructuredDataset
from flytekit.core.artifact import ArtifactQuery
from flytekit.core.data_persistence import FileAccessProvider
from flytekit.core.type_engine import TypeEngine
from flytekit.models.types import SimpleType
from flytekit.remote.remote_fs import FlytePathResolver
from flytekit.types.directory import FlyteDirectory
from flytekit.types.file import FlyteFile
from flytekit.types.iterator.json_iterator import JSONIteratorTransformer
from flytekit.types.pickle.pickle import FlytePickleTransformer
from flytekit.types.schema.types import FlyteSchema


def is_pydantic_basemodel(python_type: typing.Type) -> bool:
    """
    Checks if the python type is a pydantic BaseModel
    """
    try:
        import pydantic  # noqa: F401
    except ImportError:
        return False
    else:
        try:
            from pydantic.v1 import BaseModel
        except ImportError:
            from pydantic import BaseModel

        return issubclass(python_type, BaseModel)


def key_value_callback(_: typing.Any, param: str, values: typing.List[str]) -> typing.Optional[typing.Dict[str, str]]:
    """
    Callback for click to parse key-value pairs.
    """
    if not values:
        return None
    result = {}
    for v in values:
        if "=" not in v:
            raise click.BadParameter(f"Expected key-value pair of the form key=value, got {v}")
        k, v = v.split("=", 1)
        result[k.strip()] = v.strip()
    return result


def labels_callback(_: typing.Any, param: str, values: typing.List[str]) -> typing.Optional[typing.Dict[str, str]]:
    """
    Callback for click to parse labels.
    """
    if not values:
        return None
    result = {}
    for v in values:
        if "=" not in v:
            result[v.strip()] = ""
        else:
            k, v = v.split("=", 1)
            result[k.strip()] = v.strip()
    return result


class DirParamType(click.ParamType):
    name = "directory path"

    def convert(
        self, value: typing.Any, param: typing.Optional[click.Parameter], ctx: typing.Optional[click.Context]
    ) -> typing.Any:
        if isinstance(value, ArtifactQuery):
            return value

        # set remote_directory to false if running pyflyte run locally. This makes sure that the original
        # directory is used and not a random one.
        remote_directory = None if getattr(ctx.obj, "is_remote", False) else False
        if not FileAccessProvider.is_remote(value):
            p = pathlib.Path(value)
            if not p.exists() or not p.is_dir():
                raise click.BadParameter(f"parameter should be a valid flytedirectory path, {value}")
        return FlyteDirectory(path=value, remote_directory=remote_directory)


class StructuredDatasetParamType(click.ParamType):
    """
    TODO handle column types
    """

    name = "structured dataset path (dir/file)"

    def convert(
        self, value: typing.Any, param: typing.Optional[click.Parameter], ctx: typing.Optional[click.Context]
    ) -> typing.Any:
        if isinstance(value, ArtifactQuery):
            return value
        if isinstance(value, str):
            return StructuredDataset(uri=value)
        elif isinstance(value, StructuredDataset):
            return value
        return StructuredDataset(dataframe=value)


class FileParamType(click.ParamType):
    name = "file path"

    def convert(
        self, value: typing.Any, param: typing.Optional[click.Parameter], ctx: typing.Optional[click.Context]
    ) -> typing.Any:
        if isinstance(value, ArtifactQuery):
            return value
        # set remote_directory to false if running pyflyte run locally. This makes sure that the original
        # file is used and not a random one.
        remote_path = None if getattr(ctx.obj, "is_remote", False) else False
        if not FileAccessProvider.is_remote(value):
            p = pathlib.Path(value)
            if not p.exists() or not p.is_file():
                raise click.BadParameter(f"parameter should be a valid file path, {value}")
        return FlyteFile(path=value, remote_path=remote_path)


class PickleParamType(click.ParamType):
    name = "pickle"

    def convert(
        self, value: typing.Any, param: typing.Optional[click.Parameter], ctx: typing.Optional[click.Context]
    ) -> typing.Any:
        return value


class JSONIteratorParamType(click.ParamType):
    name = "json iterator"

    def convert(
        self, value: typing.Any, param: typing.Optional[click.Parameter], ctx: typing.Optional[click.Context]
    ) -> typing.Any:
        return value


class DateTimeType(click.DateTime):
    _NOW_FMT = "now"
    _TODAY_FMT = "today"
    _FIXED_FORMATS = [_NOW_FMT, _TODAY_FMT]
    _FLOATING_FORMATS = ["<FORMAT> - <ISO8601 duration>"]
    _ADDITONAL_FORMATS = _FIXED_FORMATS + _FLOATING_FORMATS
    _FLOATING_FORMAT_PATTERN = r"(.+)\s+([-+])\s+(.+)"

    def __init__(self):
        super().__init__()
        self.formats.extend(self._ADDITONAL_FORMATS)

    def _datetime_from_format(
        self, value: str, param: typing.Optional[click.Parameter], ctx: typing.Optional[click.Context]
    ) -> datetime.datetime:
        if value in self._FIXED_FORMATS:
            if value == self._NOW_FMT:
                return datetime.datetime.now()
            if value == self._TODAY_FMT:
                n = datetime.datetime.now()
                return datetime.datetime(n.year, n.month, n.day)
        return super().convert(value, param, ctx)

    def convert(
        self, value: typing.Any, param: typing.Optional[click.Parameter], ctx: typing.Optional[click.Context]
    ) -> typing.Any:
        if isinstance(value, ArtifactQuery):
            return value

        if isinstance(value, str) and " " in value:
            import re

            m = re.match(self._FLOATING_FORMAT_PATTERN, value)
            if m:
                parts = m.groups()
                if len(parts) != 3:
                    raise click.BadParameter(f"Expected format <FORMAT> - <ISO8601 duration>, got {value}")
                dt = self._datetime_from_format(parts[0], param, ctx)
                try:
                    delta = datetime.timedelta(seconds=parse(parts[2]))
                except Exception as e:
                    raise click.BadParameter(
                        f"Matched format {self._FLOATING_FORMATS}, but failed to parse duration {parts[2]}, error: {e}"
                    )
                if parts[1] == "-":
                    return dt - delta
                return dt + delta
            else:
                value = datetime.datetime.fromisoformat(value)

        return self._datetime_from_format(value, param, ctx)


class DurationParamType(click.ParamType):
    name = "[1:24 | :22 | 1 minute | 10 days | ...]"

    def convert(
        self, value: typing.Any, param: typing.Optional[click.Parameter], ctx: typing.Optional[click.Context]
    ) -> typing.Any:
        if isinstance(value, ArtifactQuery):
            return value
        if value is None:
            raise click.BadParameter("None value cannot be converted to a Duration type.")
        return datetime.timedelta(seconds=parse(value))


class EnumParamType(click.Choice):
    def __init__(self, enum_type: typing.Type[enum.Enum]):
        super().__init__([str(e.value) for e in enum_type])
        self._enum_type = enum_type

    def convert(
        self, value: typing.Any, param: typing.Optional[click.Parameter], ctx: typing.Optional[click.Context]
    ) -> enum.Enum:
        if isinstance(value, ArtifactQuery):
            return value
        if isinstance(value, self._enum_type):
            return value
        return self._enum_type(super().convert(value, param, ctx))


class UnionParamType(click.ParamType):
    """
    A composite type that allows for multiple types to be specified. This is used for union types.
    """

    def __init__(self, types: typing.List[click.ParamType]):
        super().__init__()
        self._types = self._sort_precedence(types)

    @property
    def name(self) -> str:
        return "|".join([t.name for t in self._types])

    @staticmethod
    def _sort_precedence(tp: typing.List[click.ParamType]) -> typing.List[click.ParamType]:
        unprocessed = []
        str_types = []
        others = []
        for t in tp:
            if isinstance(t, type(click.UNPROCESSED)):
                unprocessed.append(t)
            elif isinstance(t, type(click.STRING)):
                str_types.append(t)
            else:
                others.append(t)
        return others + str_types + unprocessed

    def convert(
        self, value: typing.Any, param: typing.Optional[click.Parameter], ctx: typing.Optional[click.Context]
    ) -> typing.Any:
        """
        Important to implement NoneType / Optional.
        Also could we just determine the click types from the python types
        """
        if isinstance(value, ArtifactQuery):
            return value
        for t in self._types:
            try:
                return t.convert(value, param, ctx)
            except Exception as e:
                logging.debug(f"Ignoring conversion error for type {t} trying other variants in Union. Error: {e}")
        raise click.BadParameter(f"Failed to convert {value} to any of the types {self._types}")


class JsonParamType(click.ParamType):
    name = "json object OR json/yaml file path"

    def __init__(self, python_type: typing.Type):
        super().__init__()
        self._python_type = python_type

    def _parse(self, value: typing.Any, param: typing.Optional[click.Parameter]):
        if type(value) == dict or type(value) == list:
            return value
        try:
            return json.loads(value)
        except Exception:  # noqa
            try:
                # We failed to load the json, so we'll try to load it as a file
                if os.path.exists(value):
                    # if the value is a yaml file, we'll try to load it as yaml
                    if value.endswith(".yaml") or value.endswith(".yml"):
                        with open(value, "r") as f:
                            return yaml.safe_load(f)
                    with open(value, "r") as f:
                        return json.load(f)
                raise
            except json.JSONDecodeError as e:
                raise click.BadParameter(f"parameter {param} should be a valid json object, {value}, error: {e}")

    def convert(
        self, value: typing.Any, param: typing.Optional[click.Parameter], ctx: typing.Optional[click.Context]
    ) -> typing.Any:
        if isinstance(value, ArtifactQuery):
            return value
        if value is None:
            raise click.BadParameter("None value cannot be converted to a Json type.")

        FLYTE_TYPES = [FlyteFile, FlyteDirectory, StructuredDataset, FlyteSchema]

        def has_nested_dataclass(t: typing.Type) -> bool:
            """
            Recursively checks whether the given type or its nested types contain any dataclass.

            This function is typically called with a dictionary or list type and will return True if
            any of the nested types within the dictionary or list is a dataclass.

            Note:
            - A single dataclass will return True.
            - The function specifically excludes certain Flyte types like FlyteFile, FlyteDirectory,
            StructuredDataset, and FlyteSchema from being considered as dataclasses. This is because
            these types are handled separately by Flyte and do not need to be converted to dataclasses.

            Args:
                t (typing.Type): The type to check for nested dataclasses.

            Returns:
                bool: True if the type or its nested types contain a dataclass, False otherwise.
            """

            if dataclasses.is_dataclass(t):
                # FlyteTypes is not supported now, we can support it in the future.
                return t not in FLYTE_TYPES

            return any(has_nested_dataclass(arg) for arg in get_args(t))

        parsed_value = self._parse(value, param)

        # We compare the origin type because the json parsed value for list or dict is always a list or dict without
        # the covariant type information.
        if type(parsed_value) == typing.get_origin(self._python_type) or type(parsed_value) == self._python_type:
            # Indexing the return value of get_args will raise an error for native dict and list types.
            # We don't support native list/dict types with nested dataclasses.
            if get_args(self._python_type) == ():
                return parsed_value
            elif isinstance(parsed_value, list) and has_nested_dataclass(get_args(self._python_type)[0]):
                j = JsonParamType(get_args(self._python_type)[0])
                return [j.convert(v, param, ctx) for v in parsed_value]
            elif isinstance(parsed_value, dict) and has_nested_dataclass(get_args(self._python_type)[1]):
                j = JsonParamType(get_args(self._python_type)[1])
                return {k: j.convert(v, param, ctx) for k, v in parsed_value.items()}

            return parsed_value

        if is_pydantic_basemodel(self._python_type):
            return self._python_type.parse_raw(json.dumps(parsed_value))  # type: ignore

        # Ensure that the python type has `from_json` function
        if not hasattr(self._python_type, "from_json"):
            self._python_type = dataclass_json(self._python_type)

        return cast(DataClassJsonMixin, self._python_type).from_json(json.dumps(parsed_value))


def modify_literal_uris(lit: Literal):
    """
    Modifies the literal object recursively to replace the URIs with the native paths.
    """
    if lit.collection:
        for l in lit.collection.literals:
            modify_literal_uris(l)
    elif lit.map:
        for k, v in lit.map.literals.items():
            modify_literal_uris(v)
    elif lit.scalar:
        if lit.scalar.blob and lit.scalar.blob.uri and lit.scalar.blob.uri.startswith(FlytePathResolver.protocol):
            lit.scalar.blob._uri = FlytePathResolver.resolve_remote_path(lit.scalar.blob.uri)
        elif lit.scalar.union:
            modify_literal_uris(lit.scalar.union.value)
        elif (
            lit.scalar.structured_dataset
            and lit.scalar.structured_dataset.uri
            and lit.scalar.structured_dataset.uri.startswith(FlytePathResolver.protocol)
        ):
            lit.scalar.structured_dataset._uri = FlytePathResolver.resolve_remote_path(
                lit.scalar.structured_dataset.uri
            )


SIMPLE_TYPE_CONVERTER: typing.Dict[SimpleType, click.ParamType] = {
    SimpleType.FLOAT: click.FLOAT,
    SimpleType.INTEGER: click.INT,
    SimpleType.STRING: click.STRING,
    SimpleType.BOOLEAN: click.BOOL,
    SimpleType.DURATION: DurationParamType(),
    SimpleType.DATETIME: DateTimeType(),
}


def literal_type_to_click_type(lt: LiteralType, python_type: typing.Type) -> click.ParamType:
    """
    Converts a Flyte LiteralType given a python_type to a click.ParamType
    """
    if lt.simple:
        if lt.simple == SimpleType.STRUCT:
            ct = JsonParamType(python_type)
            ct.name = f"JSON object {python_type.__name__}"
            return ct
        if lt.simple in SIMPLE_TYPE_CONVERTER:
            return SIMPLE_TYPE_CONVERTER[lt.simple]
        raise NotImplementedError(f"Type {lt.simple} is not supported in pyflyte run")

    if lt.enum_type:
        return EnumParamType(python_type)  # type: ignore

    if lt.structured_dataset_type:
        return StructuredDatasetParamType()

    if lt.collection_type or lt.map_value_type:
        ct = JsonParamType(python_type)
        if lt.collection_type:
            ct.name = "json list"
        else:
            ct.name = "json dictionary"
        return ct

    if lt.blob:
        if lt.blob.dimensionality == BlobType.BlobDimensionality.SINGLE:
            if lt.blob.format == FlytePickleTransformer.PYTHON_PICKLE_FORMAT:
                return PickleParamType()
            elif lt.blob.format == JSONIteratorTransformer.JSON_ITERATOR_FORMAT:
                return JSONIteratorParamType()
            return FileParamType()
        return DirParamType()

    if lt.union_type:
        cts = []
        for i in range(len(lt.union_type.variants)):
            variant = lt.union_type.variants[i]
            variant_python_type = typing.get_args(python_type)[i]
            ct = literal_type_to_click_type(variant, variant_python_type)
            cts.append(ct)
        return UnionParamType(cts)

    return click.UNPROCESSED


class FlyteLiteralConverter(object):
    name = "literal_type"

    def __init__(
        self,
        flyte_ctx: FlyteContext,
        literal_type: LiteralType,
        python_type: typing.Type,
        is_remote: bool,
    ):
        self._is_remote = is_remote
        self._literal_type = literal_type
        self._python_type = python_type
        self._flyte_ctx = flyte_ctx
        self._click_type = literal_type_to_click_type(literal_type, python_type)

    @property
    def click_type(self) -> click.ParamType:
        return self._click_type

    def is_bool(self) -> bool:
        return self.click_type == click.BOOL

    def convert(
        self, ctx: click.Context, param: typing.Optional[click.Parameter], value: typing.Any
    ) -> typing.Union[Literal, typing.Any]:
        """
        Convert the value to a Flyte Literal or a python native type. This is used by click to convert the input.
        """
        if isinstance(value, ArtifactQuery):
            return value
        try:
            # If the expected Python type is datetime.date, adjust the value to date
            if self._python_type is datetime.date:
                # Click produces datetime, so converting to date to avoid type mismatch error
                value = value.date()
            # If the input matches the default value in the launch plan, serialization can be skipped.
            if param and value == param.default:
                return None
            lit = TypeEngine.to_literal(self._flyte_ctx, value, self._python_type, self._literal_type)

            if not self._is_remote:
                # If this is used for remote execution then we need to convert it back to a python native type
                # for FlyteRemote to use it. This maybe a double conversion penalty!
                return TypeEngine.to_python_value(self._flyte_ctx, lit, self._python_type)
            return lit
        except click.BadParameter:
            raise
        except Exception as e:
            raise click.BadParameter(
                f"Failed to convert param: {param if param else 'NA'}, value: {value} to type: {self._python_type}."
                f" Reason {e}"
            ) from e
