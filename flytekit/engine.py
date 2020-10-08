import typing
from datetime import datetime, timedelta

from flytekit import typing as flyte_typing
from flytekit.annotated.promise import Promise
from flytekit.common import promise as _promise
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.interfaces.data import data_proxy as _data_proxy
from flytekit.models import literals as _literals_models, types as _type_models
from flytekit.models.core import types as _core_type_models
from flytekit.annotated import context_manager as _flyte_context


def blob_literal_to_python_value(ctx: _flyte_context.FlyteContext,
                                 blob: _literals_models.Blob) -> typing.Union[
    flyte_typing.FlyteFilePath, typing.TextIO]:
    """
    When translating a literal blob into a local Python object, we'll need to do far more complicated things, which is
    why there's an execution context as an input. We need to download the file onto the local filesystem (or construct
    a function that will do a JIT download), or open a file handle to the thing.
    """
    local_path = ctx.local_file_access.get_random_path()

    def _downloader():
        _data_proxy.Data.get_data(blob.uri, local_path, is_multipart=False)

    if blob.metadata.type.format == flyte_typing.FlyteFileFormats.CSV.value:
        return flyte_typing.FlyteCSVFilePath(local_path, _downloader, blob.uri)

    if blob.metadata.type.format == flyte_typing.FlyteFileFormats.TEXT_IO:
        _data_proxy.Data.get_data(blob.uri, local_path, is_multipart=False)
        return open(local_path, 'r')
    return flyte_typing.FlyteFilePath(local_path, _downloader, blob.uri)


def python_file_esque_to_idl_blob(ctx: _flyte_context.FlyteContext, native_value: typing.Union[
    flyte_typing.FlyteFilePath, typing.TextIO, typing.BinaryIO],
                                  blob_type: _core_type_models.BlobType) -> _literals_models.Literal:
    """
    This is where we upload files, read filehandles that were opened for Flyte, etc.
    We have to read the idl literal type given to determine the type of the incoming Python value is. We can't
    just run type(native_value). See the comments in flytekit/typing for more information on why not.
    """
    remote_path = ctx.data_proxy.get_random_path()

    # If the type is a plain old file, that means we just have to upload the file.
    if blob_type.format == flyte_typing.FlyteFileFormats.TEXT_IO:
        raise Exception("not implemented - text filehandle to blob literal")
    elif blob_type.format == flyte_typing.FlyteFileFormats.BINARY_IO:
        raise Exception("not implemented - binary filehandle to blob literal")
    else:
        # This is just a regular file, upload it and return a Blob pointing to it.
        _data_proxy.Data.put_data(
            native_value,
            remote_path,
            is_multipart=False
        )
        meta = _literals_models.BlobMetadata(type=blob_type)
        return _literals_models.Literal(
            scalar=_literals_models.Scalar(blob=_literals_models.Blob(metadata=meta, uri=remote_path)))


def literal_primitive_to_python_value(primitive: _literals_models.Primitive) -> typing.Union[
    int, float, str, bool, datetime, timedelta]:
    return primitive.value


def literal_scalar_to_python_value(ctx: _flyte_context.FlyteContext, scalar: _literals_models.Scalar) -> \
        typing.Union[
            int, float, str, bool, datetime, timedelta, flyte_typing.FlyteFilePath, None]:
    if scalar.primitive is not None:
        return scalar.primitive.value
    elif scalar.blob is not None:
        return blob_literal_to_python_value(ctx, scalar.blob)
    elif scalar.schema is not None:
        raise Exception("not yet implemented schema")
    elif scalar.none_type is not None:
        return None
    elif scalar.error is not None:
        raise Exception("error not yet implemented")
    elif scalar.generic is not None:
        raise Exception("not yet implemented - generic")


def idl_literal_to_python_value(ctx: _flyte_context.FlyteContext,
                                idl_literal: _literals_models.Literal) -> typing.Any:
    if isinstance(idl_literal, _literals_models.Literal):
        if idl_literal.scalar is not None:
            return literal_scalar_to_python_value(ctx, idl_literal.scalar)
        elif idl_literal.collection is not None:
            return [idl_literal_to_python_value(ctx, x) for x in idl_literal.collection.literals]
        elif idl_literal.map is not None:
            return {k: idl_literal_to_python_value(ctx, v) for k, v in idl_literal.map.literals.items()}
    elif isinstance(idl_literal, _literals_models.LiteralCollection):
        return [idl_literal_to_python_value(ctx, i) for i in idl_literal.literals]


def idl_literal_map_to_python_value(ctx: _flyte_context.FlyteContext,
                                    idl_literal_map: _literals_models.LiteralMap) -> typing.Dict[str, typing.Any]:
    """
    This function is only here because often we start with a LiteralMap, not a plain Literal.
    """
    return {k: idl_literal_to_python_value(ctx, v) for k, v in idl_literal_map.literals.items()}


def python_simple_value_to_idl_literal(native_value: typing.Any,
                                       simple: _type_models.SimpleType) -> _literals_models.Literal:
    if native_value is None:
        return _literals_models.Literal(scalar=_literals_models.Scalar(none_type=_literals_models.Void()))

    primitive = None
    if simple == _type_models.SimpleType.NONE:
        raise Exception("Can't translate non-None Python value to Void literal")
    elif simple == _type_models.SimpleType.INTEGER:
        if type(native_value) != int:
            raise Exception(f"Can't translate Python value {native_value} with type {type(native_value)} to Integer")
        primitive = _literals_models.Primitive(integer=native_value)

    elif simple == _type_models.SimpleType.FLOAT:
        if type(native_value) != float:
            raise Exception(f"Can't translate Python value with type {type(native_value)} to Float")
        primitive = _literals_models.Primitive(integer=native_value)

    elif simple == _type_models.SimpleType.STRING:
        if type(native_value) != str:
            raise Exception(f"Can't translate Python value with type {type(native_value)} to String")
        primitive = _literals_models.Primitive(string_value=native_value)

    elif simple == _type_models.SimpleType.BOOLEAN:
        if type(native_value) != bool:
            raise Exception(f"Can't translate Python value with type {type(native_value)} to String")
        primitive = _literals_models.Primitive(boolean=native_value)

    elif simple == _type_models.SimpleType.DATETIME:
        if type(native_value) != datetime:
            raise Exception(f"Can't translate Python value with type {type(native_value)} to Datetime")
        primitive = _literals_models.Primitive(datetime=native_value)

    elif simple == _type_models.SimpleType.DURATION:
        if type(native_value) != timedelta:
            raise Exception(f"Can't translate Python value with type {type(native_value)} to Duration")
        primitive = _literals_models.Primitive(duration=native_value)

    elif simple == _type_models.SimpleType.BINARY:
        raise Exception("not yet implemented - python native to literal binary")
    elif simple == _type_models.SimpleType.ERROR:
        raise Exception("not yet implemented - python native to literal error")
    elif simple == _type_models.SimpleType.STRUCT:
        raise Exception("not yet implemented - python native to literal struct")

    return _literals_models.Literal(scalar=_literals_models.Scalar(primitive=primitive))


def python_value_to_idl_literal(
        ctx: _flyte_context.FlyteContext, native_value: typing.Any, idl_type: _type_models.LiteralType) \
        -> typing.Union[_literals_models.Literal, _literals_models.LiteralCollection, _literals_models.LiteralMap]:
    # If the native python std value is None, but the IDL type is a Map of {"my_key": some_primitive_type}, should
    # we return Void? Or a literal map with a "my key" pointing to a Void?

    if idl_type.simple is not None:
        return python_simple_value_to_idl_literal(native_value, idl_type.simple)

    elif idl_type.schema is not None:
        raise Exception("not yet implemented python std to idl - schema")

    elif idl_type.blob is not None:
        return python_file_esque_to_idl_blob(ctx, native_value, idl_type.blob)

    elif idl_type.collection_type is not None:
        if type(native_value) != list:
            raise Exception(f"Expecting LiteralCollection but got type {type(native_value)} with value {native_value}")

        idl_literals = [python_value_to_idl_literal(ctx, x, idl_type.collection_type) for x in
                        native_value]
        return _literals_models.LiteralCollection(literals=idl_literals)

    elif idl_type.map_value_type is not None:
        if type(native_value) != dict:
            raise Exception(f"Expecting LiteralMap but got type {type(native_value)} with value {native_value}")

        incoming_keys = native_value.keys()
        if len(incoming_keys) > 0:
            if type(incoming_keys[0]) != str:
                raise Exception(f"Dictionary keys must be strings but got type {type(incoming_keys[0])}")

        idl_literals = {k: python_value_to_idl_literal(ctx, v, idl_type.map_value_type) for k, v in
                        native_value.items()}
        return _literals_models.LiteralMap(literals=idl_literals)


def binding_from_python_std(ctx: _flyte_context.FlyteContext, var_name: str, expected_literal_type,
                            t_value) -> _literals_models.Binding:
    # This handles the case where the incoming value is a workflow-level input
    if isinstance(t_value, _type_models.OutputReference):
        binding_data = _literals_models.BindingData(promise=t_value)

    # This handles the case where the given value is the output of another task
    elif isinstance(t_value, Promise):
        if not t_value.is_ready:
            binding_data = _literals_models.BindingData(promise=t_value.ref)

    elif isinstance(t_value, list) or isinstance(t_value, dict):
        # I didn't really like the list implementation below so leaving both dict and list unimplemented for now
        # When filling this part out, keep in mind detection of upstream nodes in task compilation will also need to
        # be updated.
        raise Exception("not yet handled - haytham will implement")

    # This is the scalar case - e.g. my_task(in1=5)
    else:
        # Question: Haytham/Ketan - Is it okay for me to rely on the expected idl type, which comes from the task's
        #   interface, to derive the scalar value?
        # Question: The context here is only used for complicated things like handling files. Should we
        #   support this? Or should we try to make this function simpler and only allow users to create bindings to
        #   simple scalars?
        scalar = python_value_to_idl_literal(ctx, t_value, expected_literal_type).scalar
        binding_data = _literals_models.BindingData(scalar=scalar)

    return _literals_models.Binding(var=var_name, binding=binding_data)
