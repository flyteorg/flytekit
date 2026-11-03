from typing import List, Optional

from pydantic import BaseModel

from flytekit.core.type_engine import TypeEngine, strict_type_hint_matching
from mashumaro.codecs.json import JSONDecoder


class JobConfig(BaseModel):
    """Child object to be nested inside the parent object"""
    job_config_id: str
    config_file_name: Optional[str] = "jobConfig.json"


class SchedulerConfig(BaseModel):
    """The main parent config"""
    input_storage_bucket: str
    cross_account_role_arn: str
    jobs: List[JobConfig]


class SchedulerConfigMapped(BaseModel):
    """The main parent config"""
    input_storage_bucket: str
    cross_account_role_arn: str
    jobs: dict[str, JobConfig]


def test_guessing_of_nested_pydantic():
    # Create a Pydantic model instance
    input_config = SchedulerConfig(
        input_storage_bucket="data_storage_bucket",
        cross_account_role_arn="example-role",
        jobs=[
            JobConfig(
                job_config_id="example-correlation-id-1",
                config_file_name="runConfig.json"
            )
        ]
    )
    input_config_json = input_config.model_dump_json()

    # Get the flyte type
    target_literal_type = TypeEngine.to_literal_type(SchedulerConfig)

    # get the type that Flyte Remote guesses - this is now a dataclass, not a pydantic model
    guessed_type = TypeEngine.guess_python_type(target_literal_type)

    # parse the json from pydantic, into the guessed dataclass
    decoder = JSONDecoder(guessed_type)
    input_config_dc_version = decoder.decode(input_config_json)

    # recover the dataclass back into json, and then back into pydantic, and make sure it matches.
    json_dc_version = input_config_dc_version.to_json()
    reconstituted_pydantic = SchedulerConfig.model_validate_json(json_dc_version)
    assert reconstituted_pydantic == input_config


def test_nested_pydantic_reconstruction_from_raw_json():
    existing_json = """
    {
      "input_storage_bucket": "s3://input-storage-bucket",
      "cross_account_role_arn": "cross:account:role:arn",
      "jobs": [
        {
          "job_config_id": "ecorrelation_id",
          "config_file_name": "runConfig.json"
        }
      ]}
    """

    # Get the flyte type
    target_literal_type = TypeEngine.to_literal_type(SchedulerConfig)

    # get the type that Flyte Remote guesses - this is now a dataclass, not a pydantic model
    guessed_type = TypeEngine.guess_python_type(target_literal_type)

    # parse the json from pydantic, into the guessed dataclass
    decoder = JSONDecoder(guessed_type)
    input_config_dc_version = decoder.decode(existing_json)

    # recover the dataclass back into json, and then back into pydantic, and make sure it matches.
    json_dc_version = input_config_dc_version.to_json()
    reconstituted_pydantic = SchedulerConfig.model_validate_json(json_dc_version)
    assert reconstituted_pydantic == SchedulerConfig(
        input_storage_bucket="s3://input-storage-bucket",
        cross_account_role_arn="cross:account:role:arn",
        jobs=[
            JobConfig(
                job_config_id="ecorrelation_id",
                config_file_name="runConfig.json"
            )
        ]
    )



def test_guessing_of_nested_pydantic_mapped():
    # Create a Pydantic model instance
    input_config = SchedulerConfigMapped(
        input_storage_bucket="data_storage_bucket",
        cross_account_role_arn="example-role",
        jobs={
            "job1": JobConfig(
                job_config_id="example-correlation-id-1",
                config_file_name="runConfig.json"
            )
        }
    )
    input_config_json = input_config.model_dump_json()

    # Get the flyte type
    target_literal_type = TypeEngine.to_literal_type(SchedulerConfigMapped)

    # get the type that Flyte Remote guesses - this is now a dataclass, not a pydantic model
    guessed_type = TypeEngine.guess_python_type(target_literal_type)

    # parse the json from pydantic, into the guessed dataclass
    decoder = JSONDecoder(guessed_type)
    input_config_dc_version = decoder.decode(input_config_json)

    # recover the dataclass back into json, and then back into pydantic, and make sure it matches.
    json_dc_version = input_config_dc_version.to_json()
    reconstituted_pydantic = SchedulerConfigMapped.model_validate_json(json_dc_version)
    assert reconstituted_pydantic == input_config


def test_strict_type_hint_matching_with_nested_pydantic():
    """
    Test that strict_type_hint_matching works for Pydantic models with nested types.

    If this fails, FlyteRemote.execute() will fall back to guess_python_type,
    which would then try to use a dataclass type with a Pydantic model value.
    This test is a bit unrelated to the one above, but tests additional functionality that FlyteRemote uses.
    """
    # Create a Pydantic model instance
    input_config = SchedulerConfig(
        input_storage_bucket="data_storage_bucket",
        cross_account_role_arn="example-role",
        jobs=[
            JobConfig(
                job_config_id="example-correlation-id-1",
                config_file_name="runConfig.json"
            )
        ]
    )

    # Get the target literal type (what the workflow interface expects)
    target_literal_type = TypeEngine.to_literal_type(SchedulerConfig)

    # This should succeed and return the Pydantic type
    # If it raises ValueError, FlyteRemote will fall back to guess_python_type
    matched_type = strict_type_hint_matching(input_config, target_literal_type)
    assert matched_type == SchedulerConfig, f"Expected {SchedulerConfig}, got {matched_type}"
