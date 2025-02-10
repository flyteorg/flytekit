from __future__ import annotations

from flytekit.models.core.types import EnumType
from flytekit.models.types import LiteralType, UnionType


def literal_types_match(downstream: LiteralType, upstream: LiteralType) -> bool:
    """
    Returns if two LiteralTypes are the same.
    Takes into account arbitrary ordering of enums and unions, otherwise just an equivalence check.
    """

    # If the types are exactly the same, return True
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

    # Handle enum types
    if downstream.enum_type and upstream.enum_type:
        return _enum_types_match(downstream.enum_type, upstream.enum_type)

    # Handle union types
    if downstream.union_type and upstream.union_type:
        return _union_types_match(downstream.union_type, upstream.union_type)

    # If none of the above conditions are met, the types are not castable
    return False


def _enum_types_match(downstream: EnumType, upstream: EnumType) -> bool:
    return set(upstream.values) == set(downstream.values)


def _union_types_match(downstream: UnionType, upstream: UnionType) -> bool:
    if len(downstream.variants) != len(upstream.variants):
        return False

    down_sorted = sorted(downstream.variants, key=lambda x: str(x))
    up_sorted = sorted(upstream.variants, key=lambda x: str(x))

    for downstream_variant, upstream_variant in zip(down_sorted, up_sorted):
        if not literal_types_match(downstream_variant, upstream_variant):
            return False

    return True
