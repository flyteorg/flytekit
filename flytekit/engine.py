import logging as _logging
import typing
import uuid as _uuid
from datetime import datetime, timedelta
from contextlib import contextmanager

from flytekit import __version__ as _api_version
from flytekit import typing as flyte_typing
from flytekit.annotated import type_engine
from flytekit.common import utils as _common_utils
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.configuration import (
    internal as _internal_config, sdk as _sdk_config, )
from flytekit.interfaces.data import data_proxy as _data_proxy
from flytekit.interfaces.stats.taggable import get_stats as _get_stats
from flytekit.models import literals as _literals_models, types as _type_models, interface as _interface_models
from flytekit.models.core import identifier as _identifier, types as _core_type_models
from flytekit.common import promise as _promise
from flytekit.common.exceptions import user as _user_exceptions


class ExecutionContext(object):
    def __init__(self, execution_date, tmp_dir, stats, execution_id, logging, raw_data_output_path,
                 idl_inputs: _literals_models.LiteralMap):
        """
        :param execution_date:
        :param tmp_dir:
        :param stats:
        :param execution_id:
        :param logging:
        :param raw_data_output_path: This is the prefix for S3 or
        :param idl_inputs:
        """
        self._stats = stats
        self._execution_date = execution_date
        self._working_directory = tmp_dir
        self._execution_id = execution_id
        self._logging = logging
        self._raw_data_output_path = raw_data_output_path
        self._idl_inputs = idl_inputs

    @property
    def idl_inputs(self):
        """
        :rtype: LiteralMap
        """
        return self._idl_inputs

    @property
    def stats(self):
        """
        :rtype: flytekit.interfaces.stats.taggable.TaggableStats
        """
        return self._stats

    @property
    def logging(self):
        """
        :rtype: TODO
        """
        return self._logging

    @property
    def working_directory(self):
        """
        :rtype: flytekit.common.utils.AutoDeletingTempDir
        """
        return self._working_directory

    @property
    def execution_date(self):
        """
        :rtype: datetime.datetime
        """
        return self._execution_date

    @property
    def execution_id(self):
        """
        :rtype: flytekit.models.core.identifier.WorkflowExecutionIdentifier
        """
        return self._execution_id

    @property
    def raw_data_output_path(self) -> str:
        return self._raw_data_output_path


class ExecutionContextProvider(object):

    @contextmanager
    def get_execution_environment(self, inputs: _literals_models.LiteralMap, context: typing.Dict[str, str] = None):
        with _common_utils.AutoDeletingTempDir("engine_dir") as temp_dir:
            with _common_utils.AutoDeletingTempDir("task_dir") as task_dir:
                with _data_proxy.LocalWorkingDirectoryContext(task_dir):
                    with _data_proxy.RemoteDataContext():
                        # This sets the logging level for user code and is the only place an sdk setting gets
                        # used at runtime.  Optionally, Propeller can set an internal config setting which
                        # takes precedence.
                        log_level = _internal_config.LOGGING_LEVEL.get() or _sdk_config.LOGGING_LEVEL.get()
                        _logging.getLogger().setLevel(log_level)
                        execution_context = ExecutionContext(
                            execution_id=_identifier.WorkflowExecutionIdentifier(
                                project=_internal_config.EXECUTION_PROJECT.get(),
                                domain=_internal_config.EXECUTION_DOMAIN.get(),
                                name=_internal_config.EXECUTION_NAME.get()
                            ),
                            execution_date=datetime.utcnow(),
                            stats=_get_stats(
                                # Stats metric path will be:
                                # registration_project.registration_domain.app.module.task_name.user_stats
                                # and it will be tagged with execution-level values for project/domain/wf/lp
                                "{}.{}.{}.user_stats".format(
                                    _internal_config.TASK_PROJECT.get() or _internal_config.PROJECT.get(),
                                    _internal_config.TASK_DOMAIN.get() or _internal_config.DOMAIN.get(),
                                    _internal_config.TASK_NAME.get() or _internal_config.NAME.get()
                                ),
                                tags={
                                    'exec_project': _internal_config.EXECUTION_PROJECT.get(),
                                    'exec_domain': _internal_config.EXECUTION_DOMAIN.get(),
                                    'exec_workflow': _internal_config.EXECUTION_WORKFLOW.get(),
                                    'exec_launchplan': _internal_config.EXECUTION_LAUNCHPLAN.get(),
                                    'api_version': _api_version
                                }
                            ),
                            logging=_logging,
                            tmp_dir=task_dir,
                            raw_data_output_path="/tmp/flyte/raw_output/",
                            idl_inputs=inputs,
                        )
                        yield execution_context


def _generate_local_path():
    if _data_proxy.LocalWorkingDirectoryContext.get() is None:
        raise _user_exceptions.FlyteAssertion(
            "No temporary file system is present.  Either call this method from within the "
            "context of a task or surround with a 'with LocalTestFileSystem():' block.  Or "
            "specify a path when calling this function.  Note: Cleanup is not automatic when a "
            "path is specified.")
    return _data_proxy.LocalWorkingDirectoryContext.get().get_named_tempfile(_uuid.uuid4().hex)


def blob_literal_to_python_value(execution_context: ExecutionContext,
                                 blob: _literals_models.Blob) -> typing.Union[
    flyte_typing.FlyteFilePath, typing.TextIO]:
    """
    When translating a literal blob into a local Python object, we'll need to do far more complicated things, which is
    why there's an execution context as an input. We need to download the file onto the local filesystem (or construct
    a function that will do a JIT download), or open a file handle to the thing.
    """

    # TODO: Move this into the execution context
    local_path = _generate_local_path()

    def _downloader():
        _data_proxy.Data.get_data(blob.uri, local_path, is_multipart=False)

    if blob.metadata.type.format == flyte_typing.FlyteFileFormats.CSV.value:
        return flyte_typing.FlyteCSVFilePath(local_path, _downloader, blob.uri)

    if blob.metadata.type.format == flyte_typing.FlyteFileFormats.TEXT_IO:
        _data_proxy.Data.get_data(blob.uri, local_path, is_multipart=False)
        return open(local_path, 'r')
    return flyte_typing.FlyteFilePath(local_path, _downloader, blob.uri)


def python_file_esque_to_idl_blob(execution_context: ExecutionContext, native_value: typing.Union[
    flyte_typing.FlyteFilePath, typing.TextIO, typing.BinaryIO],
                                  blob_type: _core_type_models.BlobType) -> _literals_models.Literal:
    """
    This is where we upload files, read filehandles that were opened for Flyte, etc.
    We have to read the idl literal type given to determine the type of the incoming Python value is. We can't
    just run type(native_value). See the comments in flytekit/typing for more information on why not.
    """
    # Feel like this should be provided for by the execution context
    remote_path = _data_proxy.Data.get_remote_path()

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


def literal_scalar_to_python_value(execution_context: ExecutionContext, scalar: _literals_models.Scalar) -> \
        typing.Union[
            int, float, str, bool, datetime, timedelta, flyte_typing.FlyteFilePath, None]:
    if scalar.primitive is not None:
        return scalar.primitive.value
    elif scalar.blob is not None:
        return blob_literal_to_python_value(execution_context, scalar.blob)
    elif scalar.schema is not None:
        raise Exception("not yet implemented schema")
    elif scalar.none_type is not None:
        return None
    elif scalar.error is not None:
        raise Exception("error not yet implemented")
    elif scalar.generic is not None:
        raise Exception("not yet implemented - generic")


def idl_literal_to_python_value(execution_context: ExecutionContext,
                                idl_literal: _literals_models.Literal) -> typing.Any:
    if idl_literal.scalar is not None:
        return literal_scalar_to_python_value(execution_context, idl_literal.scalar)
    elif idl_literal.collection is not None:
        return [idl_literal_to_python_value(execution_context, x) for x in idl_literal.collection.literals]
    elif idl_literal.map is not None:
        return {k: idl_literal_to_python_value(execution_context, v) for k, v in idl_literal.map.literals.items()}


def idl_literal_map_to_python_value(execution_context: ExecutionContext,
                                    idl_literal_map: _literals_models.LiteralMap) -> typing.Any:
    """
    This function is only here because often we start with a LiteralMap, not a plain Literal.
    """
    return {k: idl_literal_to_python_value(execution_context, v) for k, v in idl_literal_map.literals.items()}


def python_simple_value_to_idl_literal(native_value: typing.Any,
                                       simple: _type_models.SimpleType) -> _literals_models.Literal:
    if native_value is None:
        return _literals_models.Literal(scalar=_literals_models.Scalar(none_type=_literals_models.Void()))

    primitive = None
    if simple == _type_models.SimpleType.NONE:
        raise Exception("Can't translate non-None Python value to Void literal")
    elif simple == _type_models.SimpleType.INTEGER:
        if type(native_value) != int:
            raise Exception(f"Can't translate Python value with type {type(native_value)} to Integer")
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


def python_value_to_idl_literal(execution_context: ExecutionContext, native_value: typing.Any,
                                idl_type: _type_models.LiteralType) -> typing.Union[
    _literals_models.Literal, _literals_models.LiteralCollection, _literals_models.LiteralMap]:
    # If the native python std value is None, but the IDL type is a Map of {"my_key": some_primitive_type}, should
    # we return Void? Or a literal map with a "my key" pointing to a Void?

    if idl_type.simple is not None:
        return python_simple_value_to_idl_literal(native_value, idl_type.simple)

    elif idl_type.schema is not None:
        raise Exception("not yet implemented python std to idl - schema")

    elif idl_type.blob is not None:
        return python_file_esque_to_idl_blob(execution_context, native_value, idl_type.blob)

    elif idl_type.collection_type is not None:
        if type(native_value) != list:
            raise Exception(f"Expecting LiteralCollection but got type {type(native_value)} with value {native_value}")

        idl_literals = [python_value_to_idl_literal(execution_context, x, idl_type.collection_type) for x in
                        native_value]
        return _literals_models.LiteralCollection(literals=idl_literals)

    elif idl_type.map_value_type is not None:
        if type(native_value) != dict:
            raise Exception(f"Expecting LiteralMap but got type {type(native_value)} with value {native_value}")

        incoming_keys = native_value.keys()
        if len(incoming_keys) > 0:
            if type(incoming_keys[0]) != str:
                raise Exception(f"Dictionary keys must be strings but got type {type(incoming_keys[0])}")

        idl_literals = {k: python_value_to_idl_literal(execution_context, v, idl_type.map_value_type) for k, v in
                        native_value.items()}
        return _literals_models.LiteralMap(literals=idl_literals)


def binding_from_python_std(var_name: str, expected_literal_type, t_value) -> _literals_models.Binding:
    # This handles the case where the incoming value is a workflow-level input
    if isinstance(t_value, _promise.Input):
        # TODO: Rip out the notion of SDK types from here, from promise.Input generally
        incoming_value_type = t_value.sdk_type.to_flyte_literal_type()
        if not expected_literal_type == incoming_value_type:
            _user_exceptions.FlyteTypeException(
                incoming_value_type,
                expected_literal_type,
                additional_msg="When binding workflow input: {}".format(t_value)
            )
        promise = t_value.promise
        binding_data = _literals_models.BindingData(promise=promise)

    # This handles the case where the given value is the output of another task
    elif isinstance(t_value, _promise.NodeOutput):
        incoming_value_type = t_value.sdk_type.to_flyte_literal_type()
        if not expected_literal_type == incoming_value_type:
            _user_exceptions.FlyteTypeException(
                incoming_value_type,
                expected_literal_type,
                additional_msg="When binding node output: {}".format(t_value)
            )
        promise = t_value
        binding_data = _literals_models.BindingData(promise=promise)

    elif isinstance(t_value, list) or isinstance(t_value, dict):
        # I didn't really like the list implementation below so leaving both dict and list unimplemented for now
        # When filling this part out, keep in mind detection of upstream nodes in task compilation will also need to
        # be updated.
        raise Exception("not yet handled - haytham will implement")

    # This is the scalar case - e.g. my_task(in1=5)
    else:
        # Question: Haytham/Ketan - Is it okay for me to rely on the expected idl type, which comes from the task's
        #   interface, to derive the scalar value?
        # Question: The execution context here is only used for complicated things like handling files. Should we
        #   support this? Or should we try to make this function simpler and only allow users to create bindings to
        #   simple scalars?
        # TODO: If using the context, actually provide a real one.
        scalar = python_value_to_idl_literal(ExecutionContext(), t_value, expected_literal_type).scalar
        binding_data = _literals_models.BindingData(scalar=scalar)

    return _literals_models.Binding(var=var_name, binding=binding_data)
