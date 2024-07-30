from typing import Any

import flytekitplugins.omegaconf
import pytest
from flyteidl.core.literals_pb2 import Literal, Scalar
from flytekitplugins.omegaconf.config import OmegaConfTransformerMode
from flytekitplugins.omegaconf.dictconfig_transformer import DictConfigTransformer
from google.protobuf.struct_pb2 import Struct
from omegaconf import MISSING, DictConfig, ListConfig, OmegaConf, ValidationError
from pytest import mark, param

from flytekit import FlyteContext
from flytekit.core.type_engine import TypeEngine
from tests.conftest import ExampleConfig, ExampleNestedConfig
from tests.test_objects import TEST_CFG, MultiTypeEnum, MyConf, MySubConf, SpecialConf


@mark.parametrize(
    ("obj"),
    [
        param(
            DictConfig({}),
        ),
        param(
            DictConfig({"a": "b"}),
        ),
        param(
            DictConfig({"a": 1}),
        ),
        param(
            DictConfig({"a": MISSING}),
        ),
        param(
            DictConfig({"tuple": (1, 2, 3)}),
        ),
        param(
            ListConfig(["a", "b"]),
        ),
        param(
            ListConfig(["a", MISSING]),
        ),
        param(
            TEST_CFG,
        ),
        param(
            OmegaConf.create(ExampleNestedConfig()),
        ),
        param(
            OmegaConf.create(ExampleConfig()),
        ),
        param(
            DictConfig({"foo": MultiTypeEnum.fifo}),
        ),
        param(
            DictConfig({"foo": [MultiTypeEnum.fifo]}),
        ),
        param(DictConfig({"cfgs": [MySubConf(1), MySubConf("a"), "arg"]})),
        param(OmegaConf.structured(SpecialConf)),
    ],
)
def test_cfg_roundtrip(obj: Any) -> None:
    """Test casting DictConfig object to flyte literal and back."""

    ctx = FlyteContext.current_context()
    expected = TypeEngine.to_literal_type(type(obj))
    transformer = TypeEngine.get_transformer(type(obj))

    assert isinstance(
        transformer, flytekitplugins.omegaconf.dictconfig_transformer.DictConfigTransformer
    ) or isinstance(transformer, flytekitplugins.omegaconf.listconfig_transformer.ListConfigTransformer)

    literal = transformer.to_literal(ctx, obj, type(obj), expected)
    reconstructed = transformer.to_python_value(ctx, literal, type(obj))
    assert obj == reconstructed


def test_optional_type() -> None:
    """
    Test serialisation of DictConfigs with various optional entries, whose real types are provided by underlying
    dataclasses.
    """
    optional_obj: DictConfig = OmegaConf.structured(MySubConf())
    optional_obj1: DictConfig = OmegaConf.structured(MyConf(my_attr=MySubConf()))
    optional_obj2: DictConfig = OmegaConf.structured(MyConf())

    ctx = FlyteContext.current_context()
    expected = TypeEngine.to_literal_type(DictConfig)
    transformer = TypeEngine.get_transformer(DictConfig)

    literal = transformer.to_literal(ctx, optional_obj, DictConfig, expected)
    recon = transformer.to_python_value(ctx, literal, DictConfig)
    assert recon == optional_obj

    literal1 = transformer.to_literal(ctx, optional_obj1, DictConfig, expected)
    recon1 = transformer.to_python_value(ctx, literal1, DictConfig)
    assert recon1 == optional_obj1

    literal2 = transformer.to_literal(ctx, optional_obj2, DictConfig, expected)
    recon2 = transformer.to_python_value(ctx, literal2, DictConfig)
    assert recon2 == optional_obj2


def test_plugin_mode() -> None:
    """Test serialisation with different plugin modes configured."""
    obj = OmegaConf.structured(MyConf(my_attr=MySubConf()))

    ctx = FlyteContext.current_context()
    expected = TypeEngine.to_literal_type(DictConfig)

    with flytekitplugins.omegaconf.local_transformer_mode(OmegaConfTransformerMode.DictConfig):
        transformer = DictConfigTransformer()
        literal_slim = transformer.to_literal(ctx, obj, DictConfig, expected)
        reconstructed_slim = transformer.to_python_value(ctx, literal_slim, DictConfig)

    with flytekitplugins.omegaconf.local_transformer_mode(OmegaConfTransformerMode.DataClass):
        literal_full = transformer.to_literal(ctx, obj, DictConfig, expected)
        reconstructed_full = transformer.to_python_value(ctx, literal_full, DictConfig)

    with flytekitplugins.omegaconf.local_transformer_mode(OmegaConfTransformerMode.Auto):
        literal_semi = transformer.to_literal(ctx, obj, DictConfig, expected)
        reconstructed_semi = transformer.to_python_value(ctx, literal_semi, DictConfig)

    assert literal_slim == literal_full == literal_semi
    assert reconstructed_slim == reconstructed_full == reconstructed_semi  # comparison by value should pass

    assert OmegaConf.get_type(reconstructed_slim, "my_attr") == dict
    assert OmegaConf.get_type(reconstructed_semi, "my_attr") == MySubConf
    assert OmegaConf.get_type(reconstructed_full, "my_attr") == MySubConf

    reconstructed_slim.my_attr.my_attr = (1,)  # assign a tuple value to Union[int, str] field
    with pytest.raises(ValidationError):
        reconstructed_semi.my_attr.my_attr = (1,)
    with pytest.raises(ValidationError):
        reconstructed_full.my_attr.my_attr = (1,)


def test_auto_transformer_mode() -> None:
    """Test if auto transformer mode recovers basic information if the specified type cannot be found."""
    obj = OmegaConf.structured(MyConf(my_attr=MySubConf()))

    struct = Struct()
    struct.update(
        {
            "values.my_attr.scalar.union.value.scalar.generic.values.my_attr.scalar.union.value.scalar.primitive.integer": 1,  # noqa: E501
            "values.my_attr.scalar.union.value.scalar.generic.values.my_attr.scalar.union.type.structure.tag": "int",
            "values.my_attr.scalar.union.value.scalar.generic.values.my_attr.scalar.union.type.simple": "INTEGER",
            "values.my_attr.scalar.union.value.scalar.generic.values.list_attr.scalar.generic.values": [],
            "values.my_attr.scalar.union.value.scalar.generic.values.list_attr.scalar.generic.types": [],
            "values.my_attr.scalar.union.value.scalar.generic.types.my_attr": "typing.Union[builtins.int, builtins.str, NoneType]",  # noqa: E501
            "values.my_attr.scalar.union.value.scalar.generic.types.list_attr": "omegaconf.listconfig.ListConfig",
            "values.my_attr.scalar.union.value.scalar.generic.base_dataclass": "tests.test_objects.MySubConf",
            "values.my_attr.scalar.union.type.structure.tag": "OmegaConf DictConfig",
            "values.my_attr.scalar.union.type.simple": "STRUCT",
            "types.my_attr": "typing.Union[omegaconf.dictconfig.DictConfig, NoneType]",
            "base_dataclass": "tests.test_objects.MyConf",
        }
    )
    literal = Literal(scalar=Scalar(generic=struct))

    # construct a literal with an unknown subconfig type
    struct2 = Struct()
    struct2.update(
        {
            "values.my_attr.scalar.union.value.scalar.generic.values.my_attr.scalar.union.value.scalar.primitive.integer": 1,  # noqa: E501
            "values.my_attr.scalar.union.value.scalar.generic.values.my_attr.scalar.union.type.structure.tag": "int",
            "values.my_attr.scalar.union.value.scalar.generic.values.my_attr.scalar.union.type.simple": "INTEGER",
            "values.my_attr.scalar.union.value.scalar.generic.values.list_attr.scalar.generic.values": [],
            "values.my_attr.scalar.union.value.scalar.generic.values.list_attr.scalar.generic.types": [],
            "values.my_attr.scalar.union.value.scalar.generic.types.my_attr": "typing.Union[builtins.int, builtins.str, NoneType]",  # noqa: E501
            "values.my_attr.scalar.union.value.scalar.generic.types.list_attr": "omegaconf.listconfig.ListConfig",
            "values.my_attr.scalar.union.value.scalar.generic.base_dataclass": "tests.test_objects.MyFooConf",
            "values.my_attr.scalar.union.type.structure.tag": "OmegaConf DictConfig",
            "values.my_attr.scalar.union.type.simple": "STRUCT",
            "types.my_attr": "typing.Union[omegaconf.dictconfig.DictConfig, NoneType]",
            "base_dataclass": "tests.test_objects.MyConf",
        }
    )
    literal2 = Literal(scalar=Scalar(generic=struct2))

    ctx = FlyteContext.current_context()
    flytekitplugins.omegaconf.set_transformer_mode(OmegaConfTransformerMode.Auto)
    transformer = DictConfigTransformer()

    reconstructed = transformer.to_python_value(ctx, literal, DictConfig)
    assert obj == reconstructed

    part_reconstructed = transformer.to_python_value(ctx, literal2, DictConfig)
    assert obj == part_reconstructed
    assert OmegaConf.get_type(part_reconstructed, "my_attr") == dict

    part_reconstructed.my_attr.my_attr = (1,)  # assign a tuple value to Union[int, str] field
    with pytest.raises(ValidationError):
        reconstructed.my_attr.my_attr = (1,)
