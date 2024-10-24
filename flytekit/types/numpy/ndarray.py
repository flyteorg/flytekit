import pathlib
import typing
from collections import OrderedDict
from typing import Dict, Tuple, Type

import numpy as np
from typing_extensions import Annotated, get_args, get_origin

from flytekit.core.context_manager import FlyteContext
from flytekit.core.hash import HashMethod
from flytekit.core.type_engine import (
    AsyncTypeTransformer,
    TypeEngine,
    TypeTransformerFailedError,
)
from flytekit.models.core import types as _core_types
from flytekit.models.literals import Blob, BlobMetadata, Literal, Scalar
from flytekit.models.types import LiteralType


def extract_metadata(t: Type[np.ndarray]) -> Tuple[Type[np.ndarray], Dict[str, bool]]:
    metadata: dict = {}
    metadata_set = False

    if get_origin(t) is Annotated:
        base_type, *annotate_args = get_args(t)

        for aa in annotate_args:
            if isinstance(aa, OrderedDict):
                if metadata_set:
                    raise TypeTransformerFailedError(f"Metadata {metadata} is already specified, cannot use {aa}.")
                metadata = aa
                metadata_set = True
            elif isinstance(aa, HashMethod):
                continue
            else:
                raise TypeTransformerFailedError(f"The metadata for {t} must be of type kwtypes or HashMethod.")
        return base_type, metadata

    # Return the type itself if no metadata was found.
    return t, metadata


class NumpyArrayTransformer(AsyncTypeTransformer[np.ndarray]):
    """
    TypeTransformer that supports np.ndarray as a native type.
    """

    NUMPY_ARRAY_FORMAT = "NumpyArray"

    def __init__(self):
        super().__init__(name="Numpy Array", t=np.ndarray)

    def get_literal_type(self, t: Type[np.ndarray]) -> LiteralType:
        return LiteralType(
            blob=_core_types.BlobType(
                format=self.NUMPY_ARRAY_FORMAT,
                dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
            )
        )

    async def async_to_literal(
        self,
        ctx: FlyteContext,
        python_val: np.ndarray,
        python_type: Type[np.ndarray],
        expected: LiteralType,
    ) -> Literal:
        python_type, metadata = extract_metadata(python_type)

        meta = BlobMetadata(
            type=_core_types.BlobType(
                format=self.NUMPY_ARRAY_FORMAT,
                dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
            )
        )

        local_path = ctx.file_access.get_random_local_path() + ".npy"
        pathlib.Path(local_path).parent.mkdir(parents=True, exist_ok=True)

        # save numpy array to file
        np.save(
            file=local_path,
            arr=python_val,
            allow_pickle=metadata.get("allow_pickle", False),
        )
        remote_path = await ctx.file_access.async_put_raw_data(local_path)
        return Literal(scalar=Scalar(blob=Blob(metadata=meta, uri=remote_path)))

    async def async_to_python_value(
        self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[np.ndarray]
    ) -> np.ndarray:
        try:
            uri = lv.scalar.blob.uri
        except AttributeError:
            raise TypeTransformerFailedError(f"Cannot convert from {lv} to {expected_python_type}")

        expected_python_type, metadata = extract_metadata(expected_python_type)

        local_path = ctx.file_access.get_random_local_path()
        await ctx.file_access.async_get_data(uri, local_path, is_multipart=False)

        # load numpy array from a file
        return np.load(
            file=local_path,
            allow_pickle=metadata.get("allow_pickle", False),
            mmap_mode=metadata.get("mmap_mode"),  # type: ignore
        )

    def guess_python_type(self, literal_type: LiteralType) -> typing.Type[np.ndarray]:
        if (
            literal_type.blob is not None
            and literal_type.blob.dimensionality == _core_types.BlobType.BlobDimensionality.SINGLE
            and literal_type.blob.format == self.NUMPY_ARRAY_FORMAT
        ):
            return np.ndarray

        raise ValueError(f"Transformer {self} cannot reverse {literal_type}")


TypeEngine.register(NumpyArrayTransformer())
