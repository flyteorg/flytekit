from __future__ import annotations

from flytekit.models.core.types import BlobType, EnumType
from flytekit.models.types import LiteralType, StructuredDatasetType, UnionType


def literal_types_match(downstream: LiteralType, upstream: LiteralType) -> bool:
    """
    Determines if a downstream LiteralType can be cast to an upstream LiteralType.

    Args:
        downstream (LiteralType): The downstream LiteralType.
        upstream (LiteralType): The upstream LiteralType.

    Returns:
        bool: True if the downstream type can be cast to the upstream type, False otherwise.
    """
    # If the types are exactly the same, they are castable
    if downstream == upstream:
        return True

    if downstream.collection_type:
        if not upstream.collection_type:
            return False
        return literal_types_match(downstream.collection_type, upstream.collection_type)

    if downstream.map_value_type:
        if not upstream.map_value_type:
            return False
        return literal_types_match(downstream.map_value_type, upstream.map_value_type)

    # Handle simple types
    if downstream.simple and upstream.simple:
        # todo: need to add struct/pydantic json schema checking
        return downstream.simple == upstream.simple

    # Handle schema types
    if downstream.schema and upstream.schema:
        # trivially handle schema types
        return True

    # Handle blob types
    if downstream.blob and upstream.blob:
        return _blob_types_match(downstream.blob, upstream.blob)

    # Handle enum types
    if downstream.enum_type and upstream.enum_type:
        return _enum_types_match(downstream.enum_type, upstream.enum_type)

    # Handle structured dataset types
    if downstream.structured_dataset_type and upstream.structured_dataset_type:
        return _structured_dataset_types_match(downstream.structured_dataset_type, upstream.structured_dataset_type)

    # Handle union types
    if downstream.union_type and upstream.union_type:
        return _union_types_match(downstream.union_type, upstream.union_type)

    # If none of the above conditions are met, the types are not castable
    return False


def _blob_types_match(downstream: BlobType, upstream: BlobType) -> bool:
    return downstream.format == upstream.format and downstream.dimensionality == upstream.dimensionality


def _enum_types_match(downstream: EnumType, upstream: EnumType) -> bool:
    return set(upstream.values) == set(downstream.values)


def _structured_dataset_types_match(downstream: StructuredDatasetType, upstream: StructuredDatasetType) -> bool:
    """
    Helper function to determine if two structured dataset types are castable.

    Args:
        downstream (core.StructuredDatasetType): The downstream structured dataset type.
        upstream (core.StructuredDatasetType): The upstream structured dataset type.

    Returns:
        bool: True if the structured dataset types are castable, False otherwise.
    """
    if downstream.format != upstream.format:
        return False

    if len(downstream.columns) != len(upstream.columns):
        return False

    for downstream_col, upstream_col in zip(downstream.columns, upstream.columns):
        if downstream_col.name != upstream_col.name or not literal_types_match(
            downstream_col.literal_type, upstream_col.literal_type
        ):
            return False

    return True


def _union_types_match(downstream: UnionType, upstream: UnionType) -> bool:
    """
    Helper function to determine if two union types are castable.

    Args:
        downstream (core.UnionType): The downstream union type.
        upstream (core.UnionType): The upstream union type.

    Returns:
        bool: True if the union types are castable, False otherwise.
    """
    if len(downstream.variants) != len(upstream.variants):
        return False

    for downstream_variant, upstream_variant in zip(downstream.variants, upstream.variants):
        if not literal_types_match(downstream_variant, upstream_variant):
            return False

    return True
