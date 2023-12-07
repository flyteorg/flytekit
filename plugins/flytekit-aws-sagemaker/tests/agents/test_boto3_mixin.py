import typing
from dataclasses import dataclass

from flytekit import FlyteContext, StructuredDataset
from flytekit.core.type_engine import TypeEngine
from flytekit.interaction.string_literals import literal_map_string_repr
from flytekit.types.file import FlyteFile
from flytekitplugins.awssagemaker.agents.boto3_mixin import update_dict


@dataclass
class MyData:
    image: str
    model_name: str
    model_path: str


# TODO improve this test to actually assert outputs
def test_update_dict():
    d = update_dict(
        {"a": "{a}", "b": "{b}", "c": "{c}", "d": "{d}", "e": "{e}", "f": "{f}",
         "j": {"a": "{a}", "b": "{f}", "c": "{e}"}},
        {"a": 1, "b": "hello", "c": True, "d": 1.0, "e": [1, 2, 3], "f": {"a": "b"}})
    assert d == {'a': 1, 'b': 'hello', 'c': True, 'd': 1.0, 'e': [1, 2, 3], 'f': {'a': 'b'},
                 'j': {'a': 1, 'b': {'a': 'b'}, 'c': [1, 2, 3]}}

    lm = TypeEngine.dict_to_literal_map(FlyteContext.current_context(),
                                        {"a": 1, "b": "hello", "c": True, "d": 1.0,
                                         "e": [1, 2, 3], "f": {"a": "b"}, "g": None,
                                         "h": FlyteFile("s3://foo/bar", remote_path=False),
                                         "i": StructuredDataset(uri="s3://foo/bar")},
                                        {"a": int, "b": str, "c": bool, "d": float, "e": typing.List[int],
                                         "f": typing.Dict[str, str], "g": typing.Optional[str], "h": FlyteFile,
                                         "i": StructuredDataset})

    d = literal_map_string_repr(lm)
    print(d)

    print("{data.image}, {data.model_name}, {data.model_path}".format(
        data=MyData(image="foo", model_name="bar", model_path="baz")))
