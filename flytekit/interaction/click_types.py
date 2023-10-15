import datetime
import json
import os
import pathlib
import typing
from typing import cast

import cloudpickle
import rich_click as click
import yaml
from dataclasses_json import DataClassJsonMixin
from pytimeparse import parse

from flytekit import BlobType, FlyteContext, FlyteContextManager, Literal, LiteralType, StructuredDataset
from flytekit.core.data_persistence import FileAccessProvider
from flytekit.core.type_engine import TypeEngine
from flytekit.models.types import SimpleType
from flytekit.remote.remote_fs import FlytePathResolver
from flytekit.types.directory import FlyteDirectory
from flytekit.types.file import FlyteFile
from flytekit.types.pickle.pickle import FlytePickleTransformer


def is_pydantic_basemodel(python_type: typing.Type) -> bool:
    """
    Checks if the python type is a pydantic BaseModel
    """
    try:
        import pydantic
    except ImportError:
        return False
    else:
        return issubclass(python_type, pydantic.BaseModel)


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


class DirParamType(click.ParamType):
    name = "directory path"

    def convert(
        self, value: typing.Any, param: typing.Optional[click.Parameter], ctx: typing.Optional[click.Context]
    ) -> typing.Any:
        p = pathlib.Path(value)
        if p.exists() and p.is_dir():
            return FlyteDirectory(path=value)
        raise click.BadParameter(f"parameter should be a valid directory path, {value}")


class StructuredDatasetParamType(click.ParamType):
    name = "structured dataset path (dir/file)"

    def convert(
        self, value: typing.Any, param: typing.Optional[click.Parameter], ctx: typing.Optional[click.Context]
    ) -> typing.Any:
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
        if not FileAccessProvider.is_remote(value):
            p = pathlib.Path(value)
            if not p.exists() or not p.is_file():
                raise click.BadParameter(f"parameter should be a valid file path, {value}")
        return FlyteFile(path=value)


class PickleParamType(click.ParamType):
    name = "pickle"

    def convert(
        self, value: typing.Any, param: typing.Optional[click.Parameter], ctx: typing.Optional[click.Context]
    ) -> typing.Any:
        if os.path.isfile(value):
            return FlyteFile(path=value)
        uri = FlyteContextManager.current_context().file_access.get_random_local_path()
        with open(uri, "w+b") as outfile:
            cloudpickle.dump(value, outfile)
        return FlyteFile(path=str(pathlib.Path(uri).resolve()))


class DateTimeType(click.DateTime):
    _NOW_FMT = "now"
    _ADDITONAL_FORMATS = [_NOW_FMT]

    def __init__(self):
        super().__init__()
        self.formats.extend(self._ADDITONAL_FORMATS)

    def convert(
        self, value: typing.Any, param: typing.Optional[click.Parameter], ctx: typing.Optional[click.Context]
    ) -> typing.Any:
        if value in self._ADDITONAL_FORMATS:
            if value == self._NOW_FMT:
                return datetime.datetime.now()
        return super().convert(value, param, ctx)


class DurationParamType(click.ParamType):
    name = "[1:24 | :22 | 1 minute | 10 days | ...]"

    def convert(
        self, value: typing.Any, param: typing.Optional[click.Parameter], ctx: typing.Optional[click.Context]
    ) -> typing.Any:
        if value is None:
            raise click.BadParameter("None value cannot be converted to a Duration type.")
        return datetime.timedelta(seconds=parse(value))


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
        if value is None:
            raise click.BadParameter("None value cannot be converted to a Json type.")

        parsed_value = self._parse(value, param)

        # We compare the origin type because the json parsed value for list or dict is always a list or dict without
        # the covariant type information.
        if type(parsed_value) == typing.get_origin(self._python_type):
            return parsed_value

        if is_pydantic_basemodel(self._python_type):
            return self._python_type.parse_raw(json.dumps(parsed_value))  # type: ignore
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


class FlyteLiteralConverter(object):
    name = "literal_type"

    SIMPLE_TYPE_CONVERTER: typing.Dict[SimpleType, click.ParamType] = {
        SimpleType.FLOAT: click.FLOAT,
        SimpleType.INTEGER: click.INT,
        SimpleType.STRING: click.STRING,
        SimpleType.BOOLEAN: click.BOOL,
        SimpleType.DURATION: DurationParamType(),
        SimpleType.DATETIME: click.DateTime(),
    }

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
        self._click_type = click.UNPROCESSED

        if self._literal_type.simple:
            if self._literal_type.simple == SimpleType.STRUCT:
                self._click_type = JsonParamType(self._python_type)
                self._click_type.name = f"JSON object {self._python_type.__name__}"
            elif self._literal_type.simple in self.SIMPLE_TYPE_CONVERTER:
                self._click_type = self.SIMPLE_TYPE_CONVERTER[self._literal_type.simple]
            else:
                raise NotImplementedError(f"Type {self._literal_type.simple} is not supported in pyflyte run")

        if self._literal_type.enum_type:
            self._click_type = click.Choice(self._literal_type.enum_type.values)

        if self._literal_type.structured_dataset_type:
            self._click_type = StructuredDatasetParamType()

        if self._literal_type.collection_type or self._literal_type.map_value_type:
            self._click_type = JsonParamType(self._python_type)
            if self._literal_type.collection_type:
                self._click_type.name = "json list"
            else:
                self._click_type.name = "json dictionary"

        if self._literal_type.blob:
            if self._literal_type.blob.dimensionality == BlobType.BlobDimensionality.SINGLE:
                if self._literal_type.blob.format == FlytePickleTransformer.PYTHON_PICKLE_FORMAT:
                    self._click_type = PickleParamType()
                else:
                    self._click_type = FileParamType()
            else:
                self._click_type = DirParamType()

    @property
    def click_type(self) -> click.ParamType:
        return self._click_type

    def is_bool(self) -> bool:
        if self._literal_type.simple:
            return self._literal_type.simple == SimpleType.BOOLEAN
        return False

    def convert(
        self, ctx: click.Context, param: typing.Optional[click.Parameter], value: typing.Any
    ) -> typing.Union[Literal, typing.Any]:
        """
        Convert the value to a Flyte Literal or a python native type. This is used by click to convert the input.
        """
        try:
            lit = TypeEngine.to_literal(self._flyte_ctx, value, self._python_type, self._literal_type)
            if not self._is_remote:
                return TypeEngine.to_python_value(self._flyte_ctx, lit, self._python_type)
            return lit
        except click.BadParameter:
            raise
        except Exception as e:
            raise click.BadParameter(
                f"Failed to convert param: {param}, value: {value} to type: {self._python_type}. Reason {e}"
            ) from e
