import typing

from flytekitplugins.awssagemaker_inference.boto3_mixin import update_dict_fn

from flytekit import FlyteContext, StructuredDataset
from flytekit.core.type_engine import TypeEngine
from flytekit.interaction.string_literals import literal_map_string_repr
from flytekit.types.file import FlyteFile


def test_inputs():
    original_dict = {
        "a": "{inputs.a}",
        "b": "{inputs.b}",
        "c": "{inputs.c}",
        "d": "{inputs.d}",
        "e": "{inputs.e}",
        "f": "{inputs.f}",
        "j": {"g": "{inputs.g}", "h": "{inputs.h}", "i": "{inputs.i}"},
    }
    inputs = TypeEngine.dict_to_literal_map(
        FlyteContext.current_context(),
        {
            "a": 1,
            "b": "hello",
            "c": True,
            "d": 1.0,
            "e": [1, 2, 3],
            "f": {"a": "b"},
            "g": None,
            "h": FlyteFile("s3://foo/bar", remote_path=False),
            "i": StructuredDataset(uri="s3://foo/bar"),
        },
        {
            "a": int,
            "b": str,
            "c": bool,
            "d": float,
            "e": typing.List[int],
            "f": typing.Dict[str, str],
            "g": typing.Optional[str],
            "h": FlyteFile,
            "i": StructuredDataset,
        },
    )

    result = update_dict_fn(
        original_dict=original_dict,
        update_dict={"inputs": literal_map_string_repr(inputs)},
    )

    assert result == {
        "a": 1,
        "b": "hello",
        "c": True,
        "d": 1.0,
        "e": [1, 2, 3],
        "f": {"a": "b"},
        "j": {
            "g": None,
            "h": "s3://foo/bar",
            "i": "s3://foo/bar",
        },
    }


def test_container():
    original_dict = {"a": "{images.primary_container_image}"}
    images = {"primary_container_image": "cr.flyte.org/flyteorg/flytekit:py3.11-1.10.3"}

    result = update_dict_fn(original_dict=original_dict, update_dict={"images": images})

    assert result == {"a": "cr.flyte.org/flyteorg/flytekit:py3.11-1.10.3"}
