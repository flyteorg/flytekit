from typing import List, Optional

from pydantic import BaseModel

from flytekit import FlyteContextManager
from flytekit.core.type_engine import TypeEngine, strict_type_hint_matching


class JobConfig(BaseModel):
    """Represents a single job request from the dispatcher's 'jobs' list."""
    correlation_id: str
    config_file_name: Optional[str] = "runConfig.json"


class FlyteLauncherConfig(BaseModel):
    """The main config payload from the dispatcher to the Flyte workflow."""
    input_storage_bucket: str
    cross_account_role_arn: str
    jobs: List[JobConfig]


def test_guess_python_type_with_nested_pydantic():
    """
    Test TypeEngine.guess_python_type() with nested Pydantic models.

    This test exercises the fallback path in remote._execute() when strict_type_hint_matching fails.
    This is where the KeyError: 'type' might be occurring.
    """
    from flytekit.core.type_engine import TypeEngine

    # Get the Flyte literal type for FlyteLauncherConfig
    flyte_type = TypeEngine.to_literal_type(FlyteLauncherConfig)

    print(f"\nTesting guess_python_type with FlyteLauncherConfig")
    print(f"Flyte literal type: {flyte_type}")
    print(f"Flyte literal type simple: {flyte_type.simple}")
    print(f"Flyte literal type metadata: {flyte_type.metadata}")

    # This is what gets called in remote._execute() if strict_type_hint_matching fails
    # It passes the LiteralType directly (not .type attribute)
    guessed_type = TypeEngine.guess_python_type(flyte_type)
    print(f"Guessed type: {guessed_type}")

    # Verify we can use the guessed type for conversion
    ctx = FlyteContextManager.current_context()
    input_config = FlyteLauncherConfig(
        input_storage_bucket="data_storage_bucket",
        cross_account_role_arn="example-role",
        jobs=[
            JobConfig(
                correlation_id="example-correlation-id-1",
                config_file_name="runConfig.json"
            )
        ]
    )

    # Try to convert using the guessed type
    lit = TypeEngine.to_literal(ctx, input_config, guessed_type, flyte_type)
    print(f"Successfully created literal with guessed type")

    # Try to convert back
    python_val = TypeEngine.to_python_value(ctx, lit, guessed_type)
    print(f"Successfully converted back to Python value")

    assert python_val.input_storage_bucket == input_config.input_storage_bucket


def test_guess_python_type_with_nested_pydantic_basic():
    """
    Basic test that verifies guess_python_type() works with nested Pydantic models.

    This test specifically verifies the fix for KeyError: 'type' that occurred when
    a Pydantic model contained a List of another Pydantic model (e.g., jobs: List[JobConfig]).
    """
    import dataclasses
    from flytekit.core.type_engine import TypeEngine

    # Get the Flyte literal type for FlyteLauncherConfig
    flyte_type = TypeEngine.to_literal_type(FlyteLauncherConfig)

    # This is what gets called in remote._execute() if strict_type_hint_matching fails
    # Before the fix, this would raise KeyError: 'type'
    guessed_type = TypeEngine.guess_python_type(flyte_type)

    # Verify the guessed type is a valid dataclass
    assert dataclasses.is_dataclass(guessed_type), "Guessed type should be a dataclass"

    # Verify the guessed type has the expected fields
    field_names = {f.name for f in dataclasses.fields(guessed_type)}
    assert "input_storage_bucket" in field_names
    assert "cross_account_role_arn" in field_names
    assert "jobs" in field_names

    # Verify jobs field is a List type
    jobs_field = next(f for f in dataclasses.fields(guessed_type) if f.name == "jobs")
    assert hasattr(jobs_field.type, "__origin__"), "jobs should be a generic type (List)"

    # Create an instance using the guessed type with empty jobs list
    instance = guessed_type(
        input_storage_bucket="test_bucket",
        cross_account_role_arn="test_role",
        jobs=[]
    )
    assert instance.input_storage_bucket == "test_bucket"
    assert instance.cross_account_role_arn == "test_role"
    assert instance.jobs == []


def test_pydantic_nested_model_remote_execute_flow():
    """
    Test the full FlyteRemote.execute() flow for Pydantic models with nested types.

    This mimics what happens in remote._execute():
    1. Try strict_type_hint_matching to get the type hint
    2. If that fails, fall back to guess_python_type
    3. Use the type hint to convert the value to a literal
    4. Convert back to Python value

    This test verifies that Pydantic models with List[NestedModel] work correctly.
    """
    ctx = FlyteContextManager.current_context()

    # Create a Pydantic model instance (what the user passes to remote.execute())
    input_config = FlyteLauncherConfig(
        input_storage_bucket="data_storage_bucket",
        cross_account_role_arn="example-role",
        jobs=[
            JobConfig(
                correlation_id="example-correlation-id-1",
                config_file_name="runConfig.json"
            )
        ]
    )

    # Get the target literal type (simulates the workflow interface from Flyte backend)
    target_literal_type = TypeEngine.to_literal_type(FlyteLauncherConfig)

    # Step 1: Try strict_type_hint_matching first (like FlyteRemote._execute does)
    try:
        type_hint = strict_type_hint_matching(input_config, target_literal_type)
        print(f"strict_type_hint_matching succeeded: {type_hint}")
    except ValueError:
        # Step 2: Fall back to guess_python_type (this is where KeyError used to occur)
        print("strict_type_hint_matching failed, falling back to guess_python_type")
        type_hint = TypeEngine.guess_python_type(target_literal_type)
        print(f"guess_python_type returned: {type_hint}")

    # Step 3: Convert the input to a literal using the resolved type hint
    lit = TypeEngine.to_literal(ctx, input_config, type_hint, target_literal_type)
    print(f"Successfully created literal")

    # Step 4: Convert back to Python value (simulates what happens on the execution side)
    python_val = TypeEngine.to_python_value(ctx, lit, FlyteLauncherConfig)
    print(f"Successfully converted back to Python value")

    # Verify the round-trip conversion preserved the data
    assert python_val.input_storage_bucket == input_config.input_storage_bucket
    assert python_val.cross_account_role_arn == input_config.cross_account_role_arn
    assert len(python_val.jobs) == len(input_config.jobs)
    assert python_val.jobs[0].correlation_id == input_config.jobs[0].correlation_id
    assert python_val.jobs[0].config_file_name == input_config.jobs[0].config_file_name


def test_strict_type_hint_matching_with_nested_pydantic():
    """
    Test that strict_type_hint_matching works for Pydantic models with nested types.

    If this fails, FlyteRemote.execute() will fall back to guess_python_type,
    which would then try to use a dataclass type with a Pydantic model value.
    """
    # Create a Pydantic model instance
    input_config = FlyteLauncherConfig(
        input_storage_bucket="data_storage_bucket",
        cross_account_role_arn="example-role",
        jobs=[
            JobConfig(
                correlation_id="example-correlation-id-1",
                config_file_name="runConfig.json"
            )
        ]
    )

    # Get the target literal type (what the workflow interface expects)
    target_literal_type = TypeEngine.to_literal_type(FlyteLauncherConfig)

    print(f"\nInput value type: {type(input_config)}")
    print(f"Target literal type: {target_literal_type}")

    # This should succeed and return the Pydantic type
    # If it raises ValueError, FlyteRemote will fall back to guess_python_type
    try:
        matched_type = strict_type_hint_matching(input_config, target_literal_type)
        print(f"Matched type: {matched_type}")
        assert matched_type == FlyteLauncherConfig, f"Expected {FlyteLauncherConfig}, got {matched_type}"
    except ValueError as e:
        import pytest
        pytest.fail(
            f"strict_type_hint_matching failed with ValueError: {e}\n"
            f"This would cause FlyteRemote.execute() to fall back to guess_python_type, "
            f"leading to type mismatch errors."
        )

