import os
from pydantic import BaseModel

from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.type_engine import TypeEngine
from datetime import datetime


class Inner(BaseModel):
    a: int
    b: float
    c: str
    dt2: datetime = datetime(2022, 1, 1, 12, 0, 0)


class DC(BaseModel):
    a: int
    b: float
    c: str
    inner: Inner
    dt1: datetime = datetime(2020, 1, 1, 12, 0, 0)


def test_pydantic_v2_dataclass_to_literal():
    """Test converting Pydantic v2 models to Flyte Literal through the type engine."""
    ctx = FlyteContextManager.current_context()
    
    # Create test data
    inner = Inner(a=42, b=3.14, c="hello")
    dc = DC(a=1, b=2.5, c="world", inner=inner)
    
    # Get the literal type
    literal_type = TypeEngine.to_literal_type(DC)
    print(f"Literal type: {literal_type}")
    print(f"Literal type annotation: {literal_type.annotation}")
    print(f"Literal type structure: {literal_type.structure}")
    
    # Convert to literal
    literal = TypeEngine.to_literal(ctx, dc, DC, literal_type)
    print(f"Literal: {literal}")
    print(f"Literal binary: {literal.scalar.binary}")
    
    # Write the binary value to a local file
    # i.e. this file will deserialize with
    # flyte-cli parse-proto
    #   -f tests/flytekit/unit/core/pydantic_v2_binary_literal.msgpack -p flyteidl.core.literals_pb2.Binary
    output_file = os.path.join(os.path.dirname(__file__), "pydantic_v2_binary_literal.msgpack")
    with open(output_file, "wb") as f:
        f.write(literal.scalar.binary.serialize_to_string())
    print(f"Binary value written to: {output_file}")

    # Convert back to Python to verify round-trip works
    python_value = TypeEngine.to_python_value(ctx, literal, DC)
    print(f"Converted back: {python_value}")
    
    # Verify the data matches
    assert python_value.a == dc.a
    assert python_value.b == dc.b
    assert python_value.c == dc.c
    assert python_value.inner.a == dc.inner.a
    assert python_value.inner.b == dc.inner.b
    assert python_value.inner.c == dc.inner.c
    
    return literal