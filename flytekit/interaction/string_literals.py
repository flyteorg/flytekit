import typing

import flyteidl_rust as flyteidl

from flytekit.models.literals import Literal


# TODO(WIP):
def primitive_to_string(primitive: flyteidl.core.Primitive) -> typing.Any:
    """
    This method is used to convert a primitive to a string representation.
    """
    if isinstance(primitive.value, flyteidl.primitive.Value.Integer):
        return primitive.integer
    if isinstance(primitive.value, flyteidl.primitive.Value.FloatValue):
        return primitive.value
    # if primitive.boolean is not None:
    #     return primitive.boolean
    if isinstance(primitive.value, flyteidl.primitive.Value.StringValue):
        return primitive.value
    # if primitive.datetime is not None:
    #     return primitive.datetime.isoformat()
    # if primitive.duration is not None:
    #     return primitive.duration.total_seconds()
    raise ValueError(f"Unknown primitive type {primitive}")


# TODO(WIP):
def scalar_to_string(scalar: flyteidl.core.Scalar) -> typing.Any:
    """
    This method is used to convert a scalar to a string representation.
    """
    if scalar[0].value:
        return primitive_to_string(scalar[0].value[0])
    # if scalar.none_type:
    #     return None
    # if scalar.error:
    #     return scalar.error.message
    # if scalar.structured_dataset:
    #     return scalar.structured_dataset.uri
    # if scalar.schema:
    #     return scalar.schema.uri
    # if scalar.blob:
    #     return scalar.blob.uri
    # if scalar.binary:
    #     return base64.b64encode(scalar.binary.value)
    # if scalar.generic:
    #     return MessageToDict(scalar.generic)
    # if scalar.union:
    #     return literal_string_repr(scalar.union.value)
    raise ValueError(f"Unknown scalar type {scalar}")


def literal_string_repr(lit: flyteidl.core.Literal) -> typing.Any:
    """
    This method is used to convert a literal to a string representation. This is useful in places, where we need to
    use a shortened string representation of a literal, especially a FlyteFile, FlyteDirectory, or StructuredDataset.
    """
    if lit.value:
        return scalar_to_string(lit.value)
    if lit.collection:
        return [literal_string_repr(i) for i in lit.collection.literals]
    if lit.map:
        return {k: literal_string_repr(v) for k, v in lit.map.literals.items()}
    raise ValueError(f"Unknown literal type {lit}")


def literal_map_string_repr(
    lm: typing.Union[flyteidl.core.LiteralMap, typing.Dict[str, Literal]],
) -> typing.Dict[str, typing.Any]:
    """
    This method is used to convert a literal map to a string representation.
    """
    lmd = lm
    if isinstance(lm, flyteidl.core.LiteralMap):
        lmd = lm.literals
    return {k: literal_string_repr(v) for k, v in lmd.items()}
