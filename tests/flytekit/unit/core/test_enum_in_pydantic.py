import dataclasses
from enum import Enum

from pydantic import BaseModel

from flytekit.core.context_manager import FlyteContext
from flytekit.core.type_engine import TypeEngine


class Status(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


class Job(BaseModel):
    name: str
    status: Status


def test_pydantic_model_with_enum_ref():
    """Test that a Pydantic model with an enum field (which produces a $ref in
    the JSON schema) can be round-tripped through the type engine and that
    guess_python_type reconstructs a valid dataclass."""
    ctx = FlyteContext.current_context()
    input = Job(name="test-job", status=Status.PENDING)

    lt = TypeEngine.to_literal_type(Job)
    lv = TypeEngine.to_literal(ctx, input, Job, lt)

    assert lt
    assert lv

    # Roundtrip via the real Pydantic model
    pv = TypeEngine.to_python_value(ctx, lv, Job)
    assert pv == input

    # Guess python type from the schema (simulates pyflyte run behaviour)
    guessed = TypeEngine.guess_python_type(lt)
    assert dataclasses.is_dataclass(guessed)

    # The enum field should be reconstructed as str (enum $ref resolved)
    v = guessed(name="test-job", status="pending")
    assert v.name == "test-job"
    assert v.status == "pending"
