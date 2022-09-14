import functools
import os
import pathlib
import typing
from enum import Enum

import click
import mock
import pytest
from click.testing import CliRunner

from flytekit import FlyteContextManager
from flytekit.clis.sdk_in_container import pyflyte
from flytekit.clis.sdk_in_container.constants import CTX_CONFIG_FILE
from flytekit.clis.sdk_in_container.helpers import FLYTE_REMOTE_INSTANCE_KEY
from flytekit.clis.sdk_in_container.run import (
    REMOTE_FLAG_KEY,
    RUN_LEVEL_PARAMS_KEY,
    FileParamType,
    FlyteLiteralConverter,
    get_entities_in_file,
    run_command,
)
from flytekit.configuration import Config, Image, ImageConfig
from flytekit.core.task import task
from flytekit.core.type_engine import TypeEngine
from flytekit.models.types import SimpleType
from flytekit.remote import FlyteRemote

WORKFLOW_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)), "workflow.py")
IMPERATIVE_WORKFLOW_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)), "imperative_wf.py")
DIR_NAME = os.path.dirname(os.path.realpath(__file__))


def test_pyflyte_run_wf():
    runner = CliRunner()
    module_path = WORKFLOW_FILE
    result = runner.invoke(pyflyte.main, ["run", module_path, "my_wf", "--help"], catch_exceptions=False)

    assert result.exit_code == 0


def test_imperative_wf():
    runner = CliRunner()
    result = runner.invoke(
        pyflyte.main,
        ["run", IMPERATIVE_WORKFLOW_FILE, "wf", "--in1", "hello", "--in2", "world"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0


def test_pyflyte_run_cli():
    runner = CliRunner()
    result = runner.invoke(
        pyflyte.main,
        [
            "run",
            WORKFLOW_FILE,
            "my_wf",
            "--a",
            "1",
            "--b",
            "Hello",
            "--c",
            "1.1",
            "--d",
            '{"i":1,"a":["h","e"]}',
            "--e",
            "[1,2,3]",
            "--f",
            '{"x":1.0, "y":2.0}',
            "--g",
            os.path.join(DIR_NAME, "testdata/df.parquet"),
            "--i",
            "2020-05-01",
            "--j",
            "20H",
            "--k",
            "RED",
            "--remote",
            os.path.join(DIR_NAME, "testdata"),
            "--image",
            os.path.join(DIR_NAME, "testdata"),
            "--h",
        ],
        catch_exceptions=False,
    )
    print(result.stdout)
    assert result.exit_code == 0


@pytest.mark.parametrize(
    "input",
    ["1", os.path.join(DIR_NAME, "testdata/df.parquet"), '{"x":1.0, "y":2.0}', "2020-05-01", "RED"],
)
def test_union_type1(input):
    runner = CliRunner()
    result = runner.invoke(
        pyflyte.main,
        [
            "run",
            os.path.join(DIR_NAME, "workflow.py"),
            "test_union1",
            "--a",
            input,
        ],
        catch_exceptions=False,
    )
    print(result.stdout)
    assert result.exit_code == 0


@pytest.mark.parametrize(
    "input",
    [2.0, '{"i":1,"a":["h","e"]}', "[1, 2, 3]"],
)
def test_union_type2(input):
    runner = CliRunner()
    result = runner.invoke(
        pyflyte.main,
        [
            "run",
            os.path.join(DIR_NAME, "workflow.py"),
            "test_union2",
            "--a",
            input,
        ],
        catch_exceptions=False,
    )
    print(result.stdout)
    assert result.exit_code == 0


def test_union_type_with_invalid_input():
    runner = CliRunner()
    with pytest.raises(ValueError, match="Failed to convert python type typing.Union"):
        runner.invoke(
            pyflyte.main,
            [
                "run",
                os.path.join(DIR_NAME, "workflow.py"),
                "test_union2",
                "--a",
                "hello",
            ],
            catch_exceptions=False,
        )


def test_get_entities_in_file():
    e = get_entities_in_file(WORKFLOW_FILE)
    assert e.workflows == ["my_wf"]
    assert e.tasks == ["get_subset_df", "print_all", "show_sd", "test_union1", "test_union2"]
    assert e.all() == ["my_wf", "get_subset_df", "print_all", "show_sd", "test_union1", "test_union2"]


@pytest.mark.parametrize(
    "working_dir, wf_path",
    [
        (pathlib.Path("test_nested_wf"), os.path.join("a", "b", "c", "d", "wf.py")),
        (pathlib.Path("test_nested_wf", "a"), os.path.join("b", "c", "d", "wf.py")),
        (pathlib.Path("test_nested_wf", "a", "b"), os.path.join("c", "d", "wf.py")),
        (pathlib.Path("test_nested_wf", "a", "b", "c"), os.path.join("d", "wf.py")),
        (pathlib.Path("test_nested_wf", "a", "b", "c", "d"), os.path.join("wf.py")),
    ],
)
def test_nested_workflow(working_dir, wf_path, monkeypatch: pytest.MonkeyPatch):
    runner = CliRunner()
    base_path = os.path.dirname(os.path.realpath(__file__))
    # Change working directory without side-effects (i.e. just for this test)
    monkeypatch.chdir(os.path.join(base_path, working_dir))
    result = runner.invoke(
        pyflyte.main,
        [
            "run",
            wf_path,
            "wf_id",
            "--m",
            "wow",
        ],
        catch_exceptions=False,
    )
    assert result.stdout.strip() == "wow"
    assert result.exit_code == 0


@pytest.mark.parametrize(
    "wf_path",
    [("collection_wf.py"), ("map_wf.py"), ("dataclass_wf.py")],
)
def test_list_default_arguments(wf_path):
    runner = CliRunner()
    dir_name = os.path.dirname(os.path.realpath(__file__))
    result = runner.invoke(
        pyflyte.main,
        [
            "run",
            os.path.join(dir_name, "default_arguments", wf_path),
            "wf",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0


# default case, what comes from click if no image is specified, the click param is configured to use the default.
ic_result_1 = ImageConfig(
    default_image=Image(name="default", fqn="ghcr.io/flyteorg/mydefault", tag="py3.9-latest"),
    images=[Image(name="default", fqn="ghcr.io/flyteorg/mydefault", tag="py3.9-latest")],
)
# test that command line args are merged with the file
ic_result_2 = ImageConfig(
    default_image=None,
    images=[
        Image(name="asdf", fqn="ghcr.io/asdf/asdf", tag="latest"),
        Image(name="xyz", fqn="docker.io/xyz", tag="latest"),
        Image(name="abc", fqn="docker.io/abc", tag=None),
    ],
)
# test that command line args override the file
ic_result_3 = ImageConfig(
    default_image=None,
    images=[Image(name="xyz", fqn="ghcr.io/asdf/asdf", tag="latest"), Image(name="abc", fqn="docker.io/abc", tag=None)],
)


@pytest.mark.parametrize(
    "image_string, leaf_configuration_file_name, final_image_config",
    [
        ("ghcr.io/flyteorg/mydefault:py3.9-latest", "no_images.yaml", ic_result_1),
        ("asdf=ghcr.io/asdf/asdf:latest", "sample.yaml", ic_result_2),
        ("xyz=ghcr.io/asdf/asdf:latest", "sample.yaml", ic_result_3),
    ],
)
def test_pyflyte_run_run(image_string, leaf_configuration_file_name, final_image_config):
    @task
    def a():
        ...

    mock_click_ctx = mock.MagicMock()
    mock_remote = mock.MagicMock()
    image_tuple = (image_string,)
    image_config = ImageConfig.validate_image(None, "", image_tuple)

    run_level_params = {
        "project": "p",
        "domain": "d",
        "image_config": image_config,
    }

    pp = pathlib.Path.joinpath(
        pathlib.Path(__file__).parent.parent.parent, "configuration/configs/", leaf_configuration_file_name
    )

    obj = {
        RUN_LEVEL_PARAMS_KEY: run_level_params,
        REMOTE_FLAG_KEY: True,
        FLYTE_REMOTE_INSTANCE_KEY: mock_remote,
        CTX_CONFIG_FILE: str(pp),
    }
    mock_click_ctx.obj = obj

    def check_image(*args, **kwargs):
        assert kwargs["image_config"] == final_image_config

    mock_remote.register_script.side_effect = check_image

    run_command(mock_click_ctx, a)()


def test_file_param():
    m = mock.MagicMock()
    l = FileParamType().convert(__file__, m, m)
    assert l.local
    r = FileParamType().convert("https://tmp/file", m, m)
    assert r.local is False


class Color(Enum):
    RED = "red"
    GREEN = "green"
    BLUE = "blue"


@pytest.mark.parametrize(
    "python_type, python_value",
    [
        (typing.Union[typing.List[int], str, Color], "flyte"),
        (typing.Union[typing.List[int], str, Color], "red"),
        (typing.Union[typing.List[int], str, Color], [1, 2, 3]),
        (typing.List[int], [1, 2, 3]),
        (typing.Dict[str, int], {"flyte": 2}),
    ],
)
def test_literal_converter(python_type, python_value):
    get_upload_url_fn = functools.partial(
        FlyteRemote(Config.auto()).client.get_upload_signed_url, project="p", domain="d"
    )
    click_ctx = click.Context(click.Command("test_command"), obj={"remote": True})
    ctx = FlyteContextManager.current_context()
    lt = TypeEngine.to_literal_type(python_type)

    lc = FlyteLiteralConverter(
        click_ctx, ctx, literal_type=lt, python_type=python_type, get_upload_url_fn=get_upload_url_fn
    )

    assert lc.convert(click_ctx, ctx, python_value) == TypeEngine.to_literal(ctx, python_value, python_type, lt)


def test_enum_converter():
    pt = typing.Union[str, Color]

    get_upload_url_fn = functools.partial(FlyteRemote(Config.auto()).client.get_upload_signed_url)
    click_ctx = click.Context(click.Command("test_command"), obj={"remote": True})
    ctx = FlyteContextManager.current_context()
    lt = TypeEngine.to_literal_type(pt)
    lc = FlyteLiteralConverter(click_ctx, ctx, literal_type=lt, python_type=pt, get_upload_url_fn=get_upload_url_fn)
    union_lt = lc.convert(click_ctx, ctx, "red").scalar.union

    assert union_lt.stored_type.simple == SimpleType.STRING
    assert union_lt.stored_type.enum_type is None

    pt = typing.Union[Color, str]
    lt = TypeEngine.to_literal_type(typing.Union[Color, str])
    lc = FlyteLiteralConverter(click_ctx, ctx, literal_type=lt, python_type=pt, get_upload_url_fn=get_upload_url_fn)
    union_lt = lc.convert(click_ctx, ctx, "red").scalar.union

    assert union_lt.stored_type.simple is None
    assert union_lt.stored_type.enum_type.values == ["red", "green", "blue"]
