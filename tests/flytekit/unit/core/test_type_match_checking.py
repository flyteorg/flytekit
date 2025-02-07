import pytest
from flytekit.models.core.types import BlobType, EnumType
from flytekit.models.types import LiteralType, StructuredDatasetType, UnionType, SimpleType
from flytekit.core.type_match_checking import literal_types_match


def test_exact_match():
    lt = LiteralType(simple=SimpleType.STRING)
    assert literal_types_match(lt, lt) is True

    lt2 = LiteralType(simple=SimpleType.FLOAT)
    assert literal_types_match(lt, lt2) is False


def test_collection_type_match():
    lt1 = LiteralType(collection_type=LiteralType(SimpleType.STRING))
    lt2 = LiteralType(collection_type=LiteralType(SimpleType.STRING))
    assert literal_types_match(lt1, lt2) is True


def test_collection_type_mismatch():
    lt1 = LiteralType(collection_type=LiteralType(SimpleType.STRING))
    lt2 = LiteralType(collection_type=LiteralType(SimpleType.INTEGER))
    assert literal_types_match(lt1, lt2) is False


def test_blob_type_match():
    blob1 = LiteralType(blob=BlobType(format="csv", dimensionality=1))
    blob2 = LiteralType(blob=BlobType(format="csv", dimensionality=1))
    assert literal_types_match(blob1, blob2) is True


def test_blob_type_mismatch():
    blob1 = LiteralType(blob=BlobType(format="csv", dimensionality=1))
    blob2 = LiteralType(blob=BlobType(format="json", dimensionality=1))
    assert literal_types_match(blob1, blob2) is False


def test_enum_type_match():
    enum1 = LiteralType(enum_type=EnumType(values=["A", "B"]))
    enum2 = LiteralType(enum_type=EnumType(values=["B", "A"]))
    assert literal_types_match(enum1, enum2) is True


def test_enum_type_mismatch():
    enum1 = LiteralType(enum_type=EnumType(values=["A", "B"]))
    enum2 = LiteralType(enum_type=EnumType(values=["A", "C"]))
    assert literal_types_match(enum1, enum2) is False


def test_structured_dataset_match():
    col1 = StructuredDatasetType.DatasetColumn(name="col1", literal_type=LiteralType(simple=SimpleType.STRING))
    col2 = StructuredDatasetType.DatasetColumn(name="col2", literal_type=LiteralType(simple=SimpleType.STRUCT))

    dataset1 = LiteralType(structured_dataset_type=StructuredDatasetType(format="parquet", columns=[]))
    dataset2 = LiteralType(structured_dataset_type=StructuredDatasetType(format="parquet", columns=[]))
    assert literal_types_match(dataset1, dataset2) is True

    dataset1 = LiteralType(structured_dataset_type=StructuredDatasetType(format="parquet", columns=[col1, col2]))
    dataset2 = LiteralType(structured_dataset_type=StructuredDatasetType(format="parquet", columns=[]))
    assert literal_types_match(dataset1, dataset2) is False

    dataset1 = LiteralType(structured_dataset_type=StructuredDatasetType(format="parquet", columns=[col1, col2]))
    dataset2 = LiteralType(structured_dataset_type=StructuredDatasetType(format="parquet", columns=[col1, col2]))
    assert literal_types_match(dataset1, dataset2) is True


def test_structured_dataset_mismatch():
    dataset1 = LiteralType(structured_dataset_type=StructuredDatasetType(format="parquet", columns=[]))
    dataset2 = LiteralType(structured_dataset_type=StructuredDatasetType(format="csv", columns=[]))
    assert literal_types_match(dataset1, dataset2) is False


def test_union_type_match():
    union1 = LiteralType(union_type=UnionType(variants=[LiteralType(SimpleType.STRING), LiteralType(SimpleType.INTEGER)]))
    union2 = LiteralType(union_type=UnionType(variants=[LiteralType(SimpleType.INTEGER), LiteralType(SimpleType.STRING)]))
    assert literal_types_match(union1, union2) is True


def test_union_type_mismatch():
    union1 = LiteralType(union_type=UnionType(variants=[LiteralType(SimpleType.STRING), LiteralType(SimpleType.INTEGER)]))
    union2 = LiteralType(union_type=UnionType(variants=[LiteralType(SimpleType.STRING), LiteralType(SimpleType.BOOLEAN)]))
    assert literal_types_match(union1, union2) is False
