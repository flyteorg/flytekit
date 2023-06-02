from typing import Any, Dict, List, Optional, Type, Union

import flytekitplugins.pydantic  # noqa F401
import pytest
from flytekitplugins.pydantic import BaseModelTransformer
from pydantic import BaseModel, Extra

from flytekit import task, workflow
from flytekit.core.type_engine import TypeTransformerFailedError


class TrainConfig(BaseModel):
    """Config BaseModel for testing purposes."""

    batch_size: int = 32
    lr: float = 1e-3
    loss: str = "cross_entropy"

    class Config:
        extra = Extra.forbid


class Config(BaseModel):
    """Config BaseModel for testing purposes with an optional type hint."""

    model_config: Optional[Union[Dict[str, TrainConfig], TrainConfig]] = TrainConfig()


class ConfigRequired(BaseModel):
    """Config BaseModel for testing purposes with required attribute."""

    model_config: Union[Dict[str, TrainConfig], TrainConfig]


class ChildConfig(Config):
    """Child class config BaseModel for testing purposes."""

    d: List[int] = [1, 2, 3]


@pytest.mark.parametrize(
    "python_type,kwargs",
    [(Config, {}), (ConfigRequired, {"model_config": TrainConfig()}), (TrainConfig, {}), (TrainConfig, {})],
)
def test_transform_round_trip(python_type: Type, kwargs: Dict[str, Any]):
    """Test that a (de-)serialization roundtrip results in the identical BaseModel."""
    from flytekit.core.context_manager import FlyteContextManager

    ctx = FlyteContextManager().current_context()

    type_transformer = BaseModelTransformer()

    python_value = python_type(**kwargs)

    literal_value = type_transformer.to_literal(
        ctx,
        python_value,
        python_type,
        type_transformer.get_literal_type(python_value),
    )

    reconstructed_value = type_transformer.to_python_value(ctx, literal_value, type(python_value))

    assert reconstructed_value == python_value
    assert reconstructed_value.schema() == python_value.schema()


@pytest.mark.parametrize(
    "config_type,kwargs",
    [
        (Config, {"model_config": {"foo": TrainConfig(loss="mse")}}),
        (ConfigRequired, {"model_config": {"foo": TrainConfig(loss="mse")}}),
    ],
)
def test_pass_to_workflow(config_type: Type, kwargs: Dict[str, Any]):
    """Test passing a BaseModel instance to a workflow works."""
    cfg = config_type(**kwargs)

    @task
    def train(cfg: config_type) -> config_type:
        return cfg

    @workflow
    def wf(cfg: config_type) -> config_type:
        return train(cfg=cfg)

    returned_cfg = wf(cfg=cfg)

    assert cfg == returned_cfg


def test_pass_wrong_type_to_workflow():
    """Test passing the wrong type raises exception."""
    cfg = ChildConfig()

    @task
    def train(cfg: Config) -> Config:
        return cfg

    @workflow
    def wf(cfg: Config) -> Config:
        return train(cfg=cfg)

    with pytest.raises(TypeTransformerFailedError, match="The schema"):
        wf(cfg=cfg)
