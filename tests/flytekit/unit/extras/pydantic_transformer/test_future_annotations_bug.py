"""
Test that Pydantic BaseModel works correctly with `from __future__ import annotations`.

This addresses the issue where __annotations__ contains string literals instead of
actual type objects when future annotations are enabled, which was causing Flyte
to fail to recognize standard Python types.
"""
from __future__ import annotations

from typing import Dict, List

from pydantic import BaseModel, Field

from flytekit import task, workflow
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.type_engine import TypeEngine
from flytekit.models.types import SimpleType


# Define models at module level so they're available for get_type_hints resolution
class SimpleModel(BaseModel):
    name: str
    age: int
    height: float
    is_active: bool


class ComplexModel(BaseModel):
    tags: List[str] = Field(default_factory=lambda: ["tag1", "tag2"])
    scores: Dict[str, int] = Field(default_factory=lambda: {"math": 95, "science": 87})
    matrix: List[List[int]] = Field(default_factory=lambda: [[1, 2], [3, 4]])


class InnerModel(BaseModel):
    value: int
    label: str


class OuterModel(BaseModel):
    inner: InnerModel
    name: str


class TypedModel(BaseModel):
    string_field: str
    int_field: int
    float_field: float
    bool_field: bool
    list_field: List[str]


def test_simple_types_with_future_annotations():
    """Test that basic Python types work correctly with future annotations."""

    @task
    def process_simple_model(model: SimpleModel) -> str:
        return f"{model.name} is {model.age} years old"

    @workflow
    def simple_wf(model: SimpleModel) -> str:
        return process_simple_model(model=model)

    # Test workflow execution
    test_model = SimpleModel(name="Alice", age=30, height=5.5, is_active=True)
    result = simple_wf(model=test_model)
    assert result == "Alice is 30 years old"

    # Verify that TypeEngine correctly handles the model
    ctx = FlyteContextManager.current_context()
    lt = TypeEngine.to_literal_type(SimpleModel)
    assert lt.simple is not None
    assert lt.metadata is not None

    # Verify literal conversion works
    literal = TypeEngine.to_literal(ctx, test_model, SimpleModel, lt)
    assert literal is not None

    # Verify round-trip conversion
    converted = TypeEngine.to_python_value(ctx, literal, SimpleModel)
    assert converted.name == "Alice"
    assert converted.age == 30
    assert converted.height == 5.5
    assert converted.is_active is True


def test_complex_types_with_future_annotations():
    """Test that complex types (List, Dict) work correctly with future annotations."""

    @task
    def process_complex_model(model: ComplexModel) -> int:
        return sum(model.scores.values())

    @workflow
    def complex_wf(model: ComplexModel) -> int:
        return process_complex_model(model=model)

    # Test workflow execution
    test_model = ComplexModel()
    result = complex_wf(model=test_model)
    assert result == 182  # 95 + 87

    # Verify that TypeEngine correctly handles the model
    ctx = FlyteContextManager.current_context()
    lt = TypeEngine.to_literal_type(ComplexModel)
    assert lt.simple is not None
    assert lt.metadata is not None

    # Verify round-trip conversion
    literal = TypeEngine.to_literal(ctx, test_model, ComplexModel, lt)
    converted = TypeEngine.to_python_value(ctx, literal, ComplexModel)
    assert converted.tags == ["tag1", "tag2"]
    assert converted.scores == {"math": 95, "science": 87}
    assert converted.matrix == [[1, 2], [3, 4]]


def test_nested_basemodels_with_future_annotations():
    """Test that nested BaseModels work correctly with future annotations."""

    @task
    def process_nested_model(model: OuterModel) -> str:
        return f"{model.name}: {model.inner.label} = {model.inner.value}"

    @workflow
    def nested_wf(model: OuterModel) -> str:
        return process_nested_model(model=model)

    # Test workflow execution
    inner = InnerModel(value=42, label="answer")
    outer = OuterModel(inner=inner, name="test")
    result = nested_wf(model=outer)
    assert result == "test: answer = 42"

    # Verify round-trip conversion
    ctx = FlyteContextManager.current_context()
    lt = TypeEngine.to_literal_type(OuterModel)
    literal = TypeEngine.to_literal(ctx, outer, OuterModel, lt)
    converted = TypeEngine.to_python_value(ctx, literal, OuterModel)
    assert converted.name == "test"
    assert converted.inner.value == 42
    assert converted.inner.label == "answer"


def test_literal_type_structure_with_future_annotations():
    """Test that LiteralType structure is correctly generated with future annotations."""

    # Get the literal type
    lt = TypeEngine.to_literal_type(TypedModel)

    # Verify structure is created
    assert lt.structure is not None
    assert lt.structure.dataclass_type is not None

    # Verify that each field has the correct literal type
    dataclass_type = lt.structure.dataclass_type

    # Check that we have all expected fields
    assert "string_field" in dataclass_type
    assert "int_field" in dataclass_type
    assert "float_field" in dataclass_type
    assert "bool_field" in dataclass_type
    assert "list_field" in dataclass_type

    # Verify the types are correctly identified (not as PickleFile)
    # This is the key test - with the bug, these would be PickleFile types
    assert dataclass_type["string_field"].simple == SimpleType.STRING
    assert dataclass_type["int_field"].simple == SimpleType.INTEGER
    assert dataclass_type["float_field"].simple == SimpleType.FLOAT
    assert dataclass_type["bool_field"].simple == SimpleType.BOOLEAN
    assert dataclass_type["list_field"].collection_type is not None
