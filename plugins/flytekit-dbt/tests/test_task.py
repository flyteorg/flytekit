import os
import pathlib

import pytest
import touch
from flytekitplugins.dbt.error import DBTUnhandledError
from flytekitplugins.dbt.schema import DBTRunInput, DBTRunOutput, DBTTestInput, DBTTestOutput
from flytekitplugins.dbt.task import DBTRun, DBTTest

from flytekit import workflow
from flytekit.tools.subprocess import check_call


@pytest.fixture(scope="module", autouse=True)
def prepare_db():
    # Ensure path to database exists
    dbs_path = pathlib.Path(os.path.dirname(os.path.realpath(__file__)), "jaffle_shop", "dbs")
    dbs_path.mkdir(exist_ok=True, parents=True)
    database_file = pathlib.Path(dbs_path, "database_name.db")
    touch.touch(database_file)

    # Seed the database
    check_call(
        [
            "dbt",
            "--log-format",
            "json",
            "seed",
            "--project-dir",
            "tests/jaffle_shop",
            "--profiles-dir",
            "tests/jaffle_shop/profiles",
            "--profile",
            "jaffle_shop",
        ]
    )
    # subprocess.run(args=["dbt", "--log-format", "json", "seed", "--project-dir", "tests/jaffle_shop", "--profiles-dir", "tests/jaffle_shop/profiles", "--profile", "jaffle_shop"])

    yield

    # Delete the database file
    database_file.unlink()


# def test_simple_task2():
#     dbt_run_task = DBTRun(
#         name="test-task",
#     )

#     @workflow
#     def my_workflow() -> DBTRunOutput:
#         # run all models
#         return dbt_run_task(
#             input=DBTRunInput(
#                 project_dir="tests/jaffle_shop",
#                 profiles_dir="tests/jaffle_shop/profiles",
#                 profile="jaffle_shop",
#             )
#         )
#     result = my_workflow()
#     assert isinstance(result, DBTRunOutput)


class TestDBTRun:
    def test_simple_task(self):
        dbt_run_task = DBTRun(
            name="test-task",
        )

        @workflow
        def my_workflow() -> DBTRunOutput:
            # run all models
            return dbt_run_task(
                input=DBTRunInput(
                    project_dir="tests/jaffle_shop",
                    profiles_dir="tests/jaffle_shop/profiles",
                    profile="jaffle_shop",
                )
            )

        result = my_workflow()
        assert isinstance(result, DBTRunOutput)

    def test_incorrect_project_dir(self):
        dbt_run_task = DBTRun(
            name="test-task",
        )

        with pytest.raises(DBTUnhandledError):
            dbt_run_task(
                input=DBTRunInput(
                    project_dir=".",
                    profiles_dir="tests/jaffle_shop/profiles",
                    profile="jaffle_shop",
                )
            )

    def test_task_output(self):
        dbt_run_task = DBTRun(
            name="test-task",
        )

        project_dir = "tests/jaffle_shop"
        profiles_dir = "tests/jaffle_shop/profiles"
        profile = "jaffle_shop"

        output = dbt_run_task.execute(
            input=DBTRunInput(project_dir=project_dir, profiles_dir=profiles_dir, profile=profile)
        )

        assert output.exit_code == 0
        assert (
            output.command
            == f"dbt --log-format json run --project-dir {project_dir} --profiles-dir {profiles_dir} --profile {profile}"
        )

        with open("tests/jaffle_shop/target/run_results.json", "r") as fp:
            exp_run_result = fp.read()
        assert output.raw_run_result == exp_run_result

        with open("tests/jaffle_shop/target/manifest.json", "r") as fp:
            exp_manifest = fp.read()
        assert output.raw_manifest == exp_manifest


class TestDBTTest:
    def test_simple_task(self):
        dbt_test_task = DBTTest(
            name="test-task",
        )

        @workflow
        def test_workflow() -> DBTTestOutput:
            # run all tests
            return dbt_test_task(
                input=DBTTestInput(
                    project_dir="tests/jaffle_shop",
                    profiles_dir="tests/jaffle_shop/profiles",
                    profile="jaffle_shop",
                )
            )

        assert isinstance(test_workflow(), DBTTestOutput)

    def test_incorrect_project_dir(self):
        dbt_test_task = DBTTest(
            name="test-task",
        )

        with pytest.raises(DBTUnhandledError):
            dbt_test_task(
                input=DBTTestInput(
                    project_dir=".",
                    profiles_dir="tests/jaffle_shop/profiles",
                    profile="jaffle_shop",
                )
            )

    def test_task_output(self):
        dbt_test_task = DBTTest(
            name="test-task",
        )

        project_dir = "tests/jaffle_shop"
        profiles_dir = "tests/jaffle_shop/profiles"
        profile = "jaffle_shop"

        output = dbt_test_task.execute(
            input=DBTTestInput(project_dir=project_dir, profiles_dir=profiles_dir, profile=profile)
        )

        assert output.exit_code == 0
        assert (
            output.command
            == f"dbt --log-format json test --project-dir {project_dir} --profiles-dir {profiles_dir} --profile {profile}"
        )

        with open("tests/jaffle_shop/target/run_results.json", "r") as fp:
            exp_run_result = fp.read()
        assert output.raw_run_result == exp_run_result

        with open("tests/jaffle_shop/target/manifest.json", "r") as fp:
            exp_manifest = fp.read()
        assert output.raw_manifest == exp_manifest
