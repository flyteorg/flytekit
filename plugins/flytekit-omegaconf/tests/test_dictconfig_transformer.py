import typing as t

import pytest
from flytekitplugins.omegaconf.dictconfig_transformer import (
    check_if_valid_dictconfig,
    extract_type_and_value_maps,
    is_flattenable,
    parse_type_description,
)
from omegaconf import DictConfig, OmegaConf

from flytekit import FlyteContext


@pytest.mark.parametrize(
    "config, should_raise, match",
    [
        (OmegaConf.create({"key1": "value1", "key2": 123, "key3": True}), False, None),
        ({"key1": "value1"}, True, "Invalid type <class 'dict'>, can only serialize DictConfigs"),
        (
            OmegaConf.create({"key1.with.dot": "value1", "key2": 123}),
            True,
            "cannot be flattened as it contains non-string keys or keys containing dots",
        ),
        (
            OmegaConf.create({1: "value1", "key2": 123}),
            True,
            "cannot be flattened as it contains non-string keys or keys containing dots",
        ),
    ],
)
def test_check_if_valid_dictconfig(config, should_raise, match) -> None:
    """Test check_if_valid_dictconfig with various configurations."""
    if should_raise:
        with pytest.raises(ValueError, match=match):
            check_if_valid_dictconfig(config)
    else:
        check_if_valid_dictconfig(config)


@pytest.mark.parametrize(
    "config, should_flatten",
    [
        (OmegaConf.create({"key1": "value1", "key2": 123, "key3": True}), True),
        (OmegaConf.create({"key1": {"nested_key1": "nested_value1", "nested_key2": 456}, "key2": "value2"}), True),
        (OmegaConf.create({"key1.with.dot": "value1", "key2": 123}), False),
        (OmegaConf.create({1: "value1", "key2": 123}), False),
        (
            OmegaConf.create(
                {
                    "key1": "value1",
                    "key2": "${oc.env:VAR}",
                    "key3": OmegaConf.create({"nested_key1": "nested_value1", "nested_key2": "${oc.env:VAR}"}),
                }
            ),
            True,
        ),
        (OmegaConf.create({"key1": {"nested.key1": "value1"}}), False),
        (
            OmegaConf.create(
                {
                    "key1": "value1",
                    "key2": {"nested_key1": "nested_value1", "nested.key2": "value2"},
                    "key3": OmegaConf.create({"nested_key3": "nested_value3"}),
                }
            ),
            False,
        ),
    ],
)
def test_is_flattenable(config: DictConfig, should_flatten: bool, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test flattenable and non-flattenable DictConfigs."""
    monkeypatch.setenv("VAR", "some_value")
    assert is_flattenable(config) == should_flatten


def test_extract_type_and_value_maps_simple() -> None:
    """Test extraction of type and value maps from a simple DictConfig."""
    ctx = FlyteContext.current_context()
    config: DictConfig = OmegaConf.create({"key1": "value1", "key2": 123, "key3": True})

    type_map, value_map = extract_type_and_value_maps(ctx, config)

    expected_type_map = {"key1": "builtins.str", "key2": "builtins.int", "key3": "builtins.bool"}

    assert type_map == expected_type_map
    assert "key1" in value_map
    assert "key2" in value_map
    assert "key3" in value_map


@pytest.mark.parametrize(
    "type_desc, expected_type",
    [
        ("builtins.int", int),
        ("typing.List[builtins.int]", t.List[int]),
        ("typing.Optional[builtins.int]", t.Optional[int]),
    ],
)
def test_parse_type_description(type_desc: str, expected_type: t.Type) -> None:
    """Test parsing various type descriptions."""
    parsed_type = parse_type_description(type_desc)
    assert parsed_type == expected_type
