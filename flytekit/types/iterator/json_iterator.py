from pathlib import Path
from typing import Any, Dict, Iterator, List, Type, Union

import jsonlines
from typing_extensions import TypeAlias

from flytekit import FlyteContext, Literal, LiteralType
from flytekit.core.type_engine import (
    TypeEngine,
    TypeTransformer,
    TypeTransformerFailedError,
)
from flytekit.models.core import types as _core_types
from flytekit.models.literals import Blob, BlobMetadata, Scalar

JSONCollection: TypeAlias = Union[Dict[str, Any], List[Any]]
JSONScalar: TypeAlias = Union[bool, float, int, str]
JSON: TypeAlias = Union[JSONCollection, JSONScalar]


class JSONIterator:
    def __init__(self, reader: jsonlines.Reader):
        self._reader = reader
        self._reader_iter = reader.iter()

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return next(self._reader_iter)
        except StopIteration:
            self._reader.close()
            raise StopIteration("File handler is exhausted")


class JSONIteratorTransformer(TypeTransformer[Iterator[JSON]]):
    """
    A JSON iterator that handles conversion between an iterator/generator and a JSONL file.
    """

    JSON_ITERATOR_FORMAT = "jsonl"
    JSON_ITERATOR_METADATA = "json iterator"

    def __init__(self):
        super().__init__("JSON Iterator", Iterator[JSON])

    def get_literal_type(self, t: Type[Iterator[JSON]]) -> LiteralType:
        return LiteralType(
            blob=_core_types.BlobType(
                format=self.JSON_ITERATOR_FORMAT,
                dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
            ),
            metadata={"format": self.JSON_ITERATOR_METADATA},
        )

    def to_literal(
        self,
        ctx: FlyteContext,
        python_val: Iterator[JSON],
        python_type: Type[Iterator[JSON]],
        expected: LiteralType,
    ) -> Literal:
        local_dir = Path(ctx.file_access.get_random_local_directory())
        local_dir.mkdir(exist_ok=True)
        local_path = ctx.file_access.get_random_local_path()
        uri = str(Path(local_dir) / local_path)

        empty = True
        with open(uri, "w") as fp:
            with jsonlines.Writer(fp) as writer:
                for json_val in python_val:
                    writer.write(json_val)
                    empty = False

        if empty:
            raise ValueError("The iterator is empty.")

        meta = BlobMetadata(
            type=_core_types.BlobType(
                format=self.JSON_ITERATOR_FORMAT,
                dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
            )
        )

        return Literal(scalar=Scalar(blob=Blob(metadata=meta, uri=ctx.file_access.put_raw_data(uri))))

    def to_python_value(
        self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[Iterator[JSON]]
    ) -> JSONIterator:
        try:
            uri = lv.scalar.blob.uri
        except AttributeError:
            raise TypeTransformerFailedError(f"Cannot convert from {lv} to {expected_python_type}")

        fs = ctx.file_access.get_filesystem_for_path(uri)

        fp = fs.open(uri, "r")
        reader = jsonlines.Reader(fp)

        return JSONIterator(reader)

    def guess_python_type(self, literal_type: LiteralType) -> Type[Iterator[JSON]]:
        if (
            literal_type.blob is not None
            and literal_type.blob.dimensionality == _core_types.BlobType.BlobDimensionality.SINGLE
            and literal_type.blob.format == self.JSON_ITERATOR_FORMAT
            and literal_type.metadata == {"format": self.JSON_ITERATOR_METADATA}
        ):
            return Iterator[JSON]  # type: ignore

        raise ValueError(f"Transformer {self} cannot reverse {literal_type}.")


TypeEngine.register(JSONIteratorTransformer())
