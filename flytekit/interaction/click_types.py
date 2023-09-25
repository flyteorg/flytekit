import datetime
import json
import logging
import os
import pathlib
import typing
from dataclasses import dataclass
from typing import cast

import cloudpickle
import rich_click as click
import yaml
from dataclasses_json import DataClassJsonMixin
from pytimeparse import parse
from typing_extensions import get_args

from flytekit import Blob, BlobMetadata, BlobType, FlyteContext, FlyteContextManager, Literal, LiteralType, Scalar
from flytekit.core.data_persistence import FileAccessProvider
from flytekit.core.type_engine import TypeEngine
from flytekit.models import literals
from flytekit.models.literals import LiteralCollection, LiteralMap, Primitive, Union, Void
from flytekit.models.types import SimpleType
from flytekit.remote import FlyteRemote
from flytekit.tools import script_mode
from flytekit.types.pickle.pickle import FlytePickleTransformer


def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix) :]
    return text


@dataclass
class Directory(object):
    dir_path: str
    local_file: typing.Optional[pathlib.Path] = None
    local: bool = True


class DirParamType(click.ParamType):
    name = "directory path"

    def convert(
        self, value: typing.Any, param: typing.Optional[click.Parameter], ctx: typing.Optional[click.Context]
    ) -> typing.Any:
        if FileAccessProvider.is_remote(value):
            return Directory(dir_path=value, local=False)
        p = pathlib.Path(value)
        if p.exists() and p.is_dir():
            files = list(p.iterdir())
            if len(files) != 1:
                raise ValueError(
                    f"Currently only directories containing one file are supported, found [{len(files)}] files found in {p.resolve()}"
                )
            return Directory(dir_path=str(p), local_file=files[0].resolve())
        raise click.BadParameter(f"parameter should be a valid directory path, {value}")


@dataclass
class FileParam(object):
    filepath: str
    local: bool = True


class FileParamType(click.ParamType):
    name = "file path"

    def convert(
        self, value: typing.Any, param: typing.Optional[click.Parameter], ctx: typing.Optional[click.Context]
    ) -> typing.Any:
        if FileAccessProvider.is_remote(value):
            return FileParam(filepath=value, local=False)
        p = pathlib.Path(value)
        if p.exists() and p.is_file():
            return FileParam(filepath=str(p.resolve()))
        raise click.BadParameter(f"parameter should be a valid file path, {value}")


class PickleParamType(click.ParamType):
    name = "pickle"

    def convert(
        self, value: typing.Any, param: typing.Optional[click.Parameter], ctx: typing.Optional[click.Context]
    ) -> typing.Any:
        uri = FlyteContextManager.current_context().file_access.get_random_local_path()
        with open(uri, "w+b") as outfile:
            cloudpickle.dump(value, outfile)
        return FileParam(filepath=str(pathlib.Path(uri).resolve()))


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

    def convert(
        self, value: typing.Any, param: typing.Optional[click.Parameter], ctx: typing.Optional[click.Context]
    ) -> typing.Any:
        if value is None:
            raise click.BadParameter("None value cannot be converted to a Json type.")
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


@dataclass
class DefaultConverter(object):
    click_type: click.ParamType
    primitive_type: typing.Optional[str] = None
    scalar_type: typing.Optional[str] = None

    def convert(self, value: typing.Any, python_type_hint: typing.Optional[typing.Type] = None) -> Scalar:
        if self.primitive_type:
            return Scalar(primitive=Primitive(**{self.primitive_type: value}))
        if self.scalar_type:
            return Scalar(**{self.scalar_type: value})

        raise NotImplementedError("Not implemented yet!")


class FlyteLiteralConverter(object):
    name = "literal_type"

    SIMPLE_TYPE_CONVERTER: typing.Dict[SimpleType, DefaultConverter] = {
        SimpleType.FLOAT: DefaultConverter(click.FLOAT, primitive_type="float_value"),
        SimpleType.INTEGER: DefaultConverter(click.INT, primitive_type="integer"),
        SimpleType.STRING: DefaultConverter(click.STRING, primitive_type="string_value"),
        SimpleType.BOOLEAN: DefaultConverter(click.BOOL, primitive_type="boolean"),
        SimpleType.DURATION: DefaultConverter(DurationParamType(), primitive_type="duration"),
        SimpleType.DATETIME: DefaultConverter(click.DateTime(), primitive_type="datetime"),
    }

    def __init__(
        self,
        flyte_ctx: FlyteContext,
        literal_type: LiteralType,
        python_type: typing.Type,
        get_upload_url_fn: typing.Callable,
        is_remote: bool,
        remote_instance_accessor: typing.Callable[[], FlyteRemote] = None,
    ):
        self._is_remote = is_remote
        self._literal_type = literal_type
        self._python_type = python_type
        self._create_upload_fn = get_upload_url_fn
        self._flyte_ctx = flyte_ctx
        self._click_type = click.UNPROCESSED
        self._remote_instance_accessor = remote_instance_accessor

        if self._literal_type.simple:
            if self._literal_type.simple == SimpleType.STRUCT:
                self._click_type = JsonParamType()
                self._click_type.name = f"JSON object {self._python_type.__name__}"
            elif self._literal_type.simple not in self.SIMPLE_TYPE_CONVERTER:
                raise NotImplementedError(f"Type {self._literal_type.simple} is not supported in pyflyte run")
            else:
                self._converter = self.SIMPLE_TYPE_CONVERTER[self._literal_type.simple]
                self._click_type = self._converter.click_type

        if self._literal_type.enum_type:
            self._converter = self.SIMPLE_TYPE_CONVERTER[SimpleType.STRING]
            self._click_type = click.Choice(self._literal_type.enum_type.values)

        if self._literal_type.structured_dataset_type:
            self._click_type = DirParamType()

        if self._literal_type.collection_type or self._literal_type.map_value_type:
            self._click_type = JsonParamType()
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

    def get_uri_for_dir(
        self, ctx: typing.Optional[click.Context], value: Directory, remote_filename: typing.Optional[str] = None
    ):
        uri = value.dir_path

        if self._is_remote and value.local:
            md5, _ = script_mode.hash_file(value.local_file)
            if not remote_filename:
                remote_filename = value.local_file.name
            remote = self._remote_instance_accessor()
            _, native_url = remote.upload_file(value.local_file)
            uri = native_url[: -len(remote_filename)]

        return uri

    def convert_to_structured_dataset(
        self, ctx: typing.Optional[click.Context], param: typing.Optional[click.Parameter], value: Directory
    ) -> Literal:

        uri = self.get_uri_for_dir(ctx, value, "00000.parquet")

        lit = Literal(
            scalar=Scalar(
                structured_dataset=literals.StructuredDataset(
                    uri=uri,
                    metadata=literals.StructuredDatasetMetadata(
                        structured_dataset_type=self._literal_type.structured_dataset_type
                    ),
                ),
            ),
        )

        return lit

    def convert_to_blob(
        self,
        ctx: typing.Optional[click.Context],
        param: typing.Optional[click.Parameter],
        value: typing.Union[Directory, FileParam],
    ) -> Literal:
        if isinstance(value, Directory):
            uri = self.get_uri_for_dir(ctx, value)
        else:
            uri = value.filepath
            if self._is_remote and value.local:
                fp = pathlib.Path(value.filepath)
                remote = self._remote_instance_accessor()
                _, uri = remote.upload_file(fp)

        lit = Literal(
            scalar=Scalar(
                blob=Blob(
                    metadata=BlobMetadata(type=self._literal_type.blob),
                    uri=uri,
                ),
            ),
        )

        return lit

    def convert_to_union(
        self, ctx: typing.Optional[click.Context], param: typing.Optional[click.Parameter], value: typing.Any
    ) -> Literal:
        lt = self._literal_type

        # handle case where Union type has NoneType and the value is None
        has_none_type = any(v.simple == 0 for v in self._literal_type.union_type.variants)
        if has_none_type and value is None:
            return Literal(scalar=Scalar(none_type=Void()))

        for i in range(len(self._literal_type.union_type.variants)):
            variant = self._literal_type.union_type.variants[i]
            python_type = get_args(self._python_type)[i]
            converter = FlyteLiteralConverter(
                self._flyte_ctx,
                variant,
                python_type,
                self._create_upload_fn,
                self._is_remote,
                self._remote_instance_accessor,
            )
            try:
                # Here we use click converter to convert the input in command line to native python type,
                # and then use flyte converter to convert it to literal.
                python_val = converter._click_type.convert(value, param, ctx)
                literal = converter.convert_to_literal(ctx, param, python_val)
                return Literal(scalar=Scalar(union=Union(literal, variant)))
            except (Exception or AttributeError) as e:
                logging.debug(f"Failed to convert python type {python_type} to literal type {variant}", e)
        raise ValueError(f"Failed to convert python type {self._python_type} to literal type {lt}")

    def convert_to_list(
        self, ctx: typing.Optional[click.Context], param: typing.Optional[click.Parameter], value: list
    ) -> Literal:
        """
        Convert a python list into a Flyte Literal
        """
        if not value:
            raise click.BadParameter("Expected non-empty list")
        if not isinstance(value, list):
            raise click.BadParameter(f"Expected json list '[...]', parsed value is {type(value)}")
        converter = FlyteLiteralConverter(
            self._flyte_ctx,
            self._literal_type.collection_type,
            type(value[0]),
            self._create_upload_fn,
            self._is_remote,
            self._remote_instance_accessor,
        )
        lt = Literal(collection=LiteralCollection([]))
        for v in value:
            click_val = converter._click_type.convert(v, param, ctx)
            lt.collection.literals.append(converter.convert_to_literal(ctx, param, click_val))
        return lt

    def convert_to_map(
        self, ctx: typing.Optional[click.Context], param: typing.Optional[click.Parameter], value: dict
    ) -> Literal:
        """
        Convert a python dict into a Flyte Literal.
        It is assumed that the click parameter type is a JsonParamType. The map is also assumed to be univariate.
        """
        if not value:
            raise click.BadParameter("Expected non-empty dict")
        if not isinstance(value, dict):
            raise click.BadParameter(f"Expected json dict '{{...}}', parsed value is {type(value)}")
        converter = FlyteLiteralConverter(
            self._flyte_ctx,
            self._literal_type.map_value_type,
            type(value[list(value.keys())[0]]),
            self._create_upload_fn,
            self._is_remote,
            self._remote_instance_accessor,
        )
        lt = Literal(map=LiteralMap({}))
        for k, v in value.items():
            click_val = converter._click_type.convert(v, param, ctx)
            lt.map.literals[k] = converter.convert_to_literal(ctx, param, click_val)
        return lt

    def convert_to_struct(
        self,
        ctx: typing.Optional[click.Context],
        param: typing.Optional[click.Parameter],
        value: typing.Union[dict, typing.Any],
    ) -> Literal:
        """
        Convert the loaded json object to a Flyte Literal struct type.
        """
        if type(value) != self._python_type:
            if is_pydantic_basemodel(self._python_type):
                o = self._python_type.parse_raw(json.dumps(value))  # type: ignore
            else:
                o = cast(DataClassJsonMixin, self._python_type).from_json(json.dumps(value))
        else:
            o = value
        return TypeEngine.to_literal(self._flyte_ctx, o, self._python_type, self._literal_type)

    def convert_to_literal(
        self, ctx: typing.Optional[click.Context], param: typing.Optional[click.Parameter], value: typing.Any
    ) -> Literal:
        if self._literal_type.structured_dataset_type:
            return self.convert_to_structured_dataset(ctx, param, value)

        if self._literal_type.blob:
            return self.convert_to_blob(ctx, param, value)

        if self._literal_type.collection_type:
            return self.convert_to_list(ctx, param, value)

        if self._literal_type.map_value_type:
            return self.convert_to_map(ctx, param, value)

        if self._literal_type.union_type:
            return self.convert_to_union(ctx, param, value)

        if self._literal_type.simple or self._literal_type.enum_type:
            if self._literal_type.simple and self._literal_type.simple == SimpleType.STRUCT:
                return self.convert_to_struct(ctx, param, value)
            return Literal(scalar=self._converter.convert(value, self._python_type))

        if self._literal_type.schema:
            raise DeprecationWarning("Schema Types are not supported in pyflyte run. Use StructuredDataset instead.")

        raise NotImplementedError(
            f"CLI parsing is not available for Python Type:`{self._python_type}`, LiteralType:`{self._literal_type}`."
        )

    def convert(
        self, ctx: click.Context, param: typing.Optional[click.Parameter], value: typing.Any
    ) -> typing.Union[Literal, typing.Any]:
        """
        Convert the value to a Flyte Literal or a python native type. This is used by click to convert the input.
        """
        try:
            lit = self.convert_to_literal(ctx, param, value)
            if not self._is_remote:
                return TypeEngine.to_python_value(self._flyte_ctx, lit, self._python_type)
            return lit
        except click.BadParameter:
            raise
        except Exception as e:
            raise click.BadParameter(f"Failed to convert param {param}, {value} to {self._python_type}") from e


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
