import typing as t

import pytest
from flytekitplugins.omegaconf.type_information import extract_node_type
from omegaconf import DictConfig, ListConfig, OmegaConf

from tests.conftest import ExampleConfig, ExampleConfigWithNonAnnotatedSubtree


class TestExtractNodeType:
    def test_extract_type_and_string_representation(self) -> None:
        """Tests type extraction and string representation."""

        python_val = OmegaConf.structured(ExampleConfig(union_key="1337", optional_key=None))

        # test int
        node_type, type_name = extract_node_type(python_val, key="int_key")
        assert node_type == int
        assert type_name == "builtins.int"

        # test union
        node_type, type_name = extract_node_type(python_val, key="union_key")
        assert node_type == t.Union[int, str]
        assert type_name == "typing.Union[builtins.int, builtins.str]"

        # test any
        node_type, type_name = extract_node_type(python_val, key="any_key")
        assert node_type == str
        assert type_name == "builtins.str"

        # test optional
        node_type, type_name = extract_node_type(python_val, key="optional_key")
        assert node_type == t.Optional[int]
        assert type_name == "typing.Union[builtins.int, NoneType]"

        # test dictconfig
        node_type, type_name = extract_node_type(python_val, key="dictconfig_key")
        assert node_type == DictConfig
        assert type_name == "omegaconf.dictconfig.DictConfig"

        # test listconfig
        node_type, type_name = extract_node_type(python_val, key="listconfig_key")
        assert node_type == ListConfig
        assert type_name == "omegaconf.listconfig.ListConfig"

        # test optional dictconfig
        node_type, type_name = extract_node_type(python_val, key="optional_dictconfig_key")
        assert node_type == t.Optional[DictConfig]
        assert type_name == "typing.Union[omegaconf.dictconfig.DictConfig, NoneType]"

    def test_raises_nonannotated_subtree(self) -> None:
        """Test that trying to infer type of a non-annotated subtree raises an error."""

        python_val = OmegaConf.structured(ExampleConfigWithNonAnnotatedSubtree())
        node_type, type_name = extract_node_type(python_val, key="annotated_key")
        assert node_type == DictConfig

        # When we try to infer unnanotated subtree combined with typed subtree, we should raise
        with pytest.raises(ValueError):
            extract_node_type(python_val, "unnanotated_key")

    def test_single_unnanotated_node(self) -> None:
        """Test that inferring a fully unnanotated node works by inferring types from runtime values."""

        python_val = OmegaConf.create({"unannotated_dictconfig_key": {"unnanotated_int_key": 2}})
        node_type, type_name = extract_node_type(python_val, key="unannotated_dictconfig_key")
        assert node_type == DictConfig

        python_val = python_val.unannotated_dictconfig_key
        node_type, type_name = extract_node_type(python_val, key="unnanotated_int_key")
        assert node_type == int
