## ====================================================================================
#  Flyte directory and files creation
## ====================================================================================

import os
from typing import Any, Dict, Optional, Type, TypeVar, Union

from flytekit.core import context_manager, type_engine
from flytekit.models import literals
from flytekit.models.core import types as core_types
from flytekit.types.directory import types as flyte_directory_types
from flytekit.types.file import file as flyte_file

FlytePath = TypeVar("FlytePath", flyte_file.FlyteFile, flyte_directory_types.FlyteDirectory)


def upload_to_s3(flytepath: FlytePath) -> None:
    """Upload a FlytePath to S3"""
    ctx = context_manager.FlyteContextManager.current_context()
    if flytepath.remote_path is None:
        flytepath.remote_path = remote_path = ctx.file_access.get_random_remote_path(flytepath.path)
    is_multipart = isinstance(flytepath, flyte_directory_types.FlyteDirectory)
    ctx.file_access.put_data(flytepath.path, remote_path, is_multipart=is_multipart)


def make_flytepath(path: Union[str, os.PathLike], flyte_type: Type[FlytePath]) -> Optional[FlytePath]:
    """create a FlyteDirectory from a path"""
    context = context_manager.FlyteContextManager.current_context()
    transformer = get_flyte_transformer(flyte_type)
    dimensionality = get_flyte_dimensionality(flyte_type)
    literal = make_literal(uri=path, dimensionality=dimensionality)
    out_dir = transformer.to_python_value(context, literal, flyte_type)
    return out_dir


def get_flyte_transformer(
    flyte_type: Type[FlytePath],
) -> type_engine.TypeTransformer[FlytePath]:
    """get the transformer for a given flyte type"""
    return FLYTE_TRANSFORMERS[flyte_type]


FLYTE_TRANSFORMERS: Dict[Type, type_engine.TypeTransformer] = {
    flyte_file.FlyteFile: flyte_file.FlyteFilePathTransformer(),
    flyte_directory_types.FlyteDirectory: flyte_directory_types.FlyteDirToMultipartBlobTransformer(),
}


def get_flyte_dimensionality(
    flyte_type: Type[FlytePath],
) -> str:
    """get the transformer for a given flyte type"""
    return FLYTE_DIMENSIONALITY[flyte_type]


FLYTE_DIMENSIONALITY: Dict[Type, Any] = {
    flyte_file.FlyteFile: core_types.BlobType.BlobDimensionality.SINGLE,
    flyte_directory_types.FlyteDirectory: core_types.BlobType.BlobDimensionality.MULTIPART,
}


def make_literal(
    uri: Union[str, os.PathLike],
    dimensionality,
) -> literals.Literal:
    scalar = make_scalar(uri, dimensionality)
    return literals.Literal(scalar=scalar)  # type: ignore


def make_scalar(
    uri: Union[str, os.PathLike],
    dimensionality,
) -> literals.Scalar:
    blobtype = core_types.BlobType(format="", dimensionality=dimensionality)
    blob = literals.Blob(metadata=literals.BlobMetadata(type=blobtype), uri=uri)
    return literals.Scalar(blob=blob)  # type: ignore
