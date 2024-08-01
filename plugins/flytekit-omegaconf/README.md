# Flytekit OmegaConf Plugin

Flytekit python natively supports serialization of many data types for exchanging information between tasks.
The Flytekit OmegaConf Plugin extends these by the `DictConfig` type from the
[OmegaConf package](https://omegaconf.readthedocs.io/) as well as related types
that are being used by the [hydra package](https://hydra.cc/) for configuration management.

## Task example
```
from dataclasses import dataclass
import flytekitplugins.omegaconf  # noqa F401
from flytekit import task, workflow
from omegaconf import DictConfig

@dataclass
class MySimpleConf:
    _target_: str = "lightning_module.MyEncoderModule"
    learning_rate: float = 0.0001

@task
def my_task(cfg: DictConfig) -> None:
    print(f"Doing things with {cfg.learning_rate=}")


@workflow
def pipeline(cfg: DictConfig) -> None:
    my_task(cfg=cfg)


if __name__ == "__main__":
    from omegaconf import OmegaConf

    cfg = OmegaConf.structured(MySimpleConf)
    pipeline(cfg=cfg)
```

## Transformer configuration

The transformer can be set to one of three modes:

`Dataclass` - This mode should be used with a StructuredConfig and will reconstruct the config from the matching dataclass
during deserialisation in order to make typing information from the dataclass and continued validation thereof available.
This requires the dataclass definition to be available via python import in the Flyte execution environment in which
objects are (de-)serialised.

`DictConfig` - This mode will deserialize the config into a DictConfig object. In particular, dataclasses are translated
into DictConfig objects and only primitive types are being checked. The definition of underlying dataclasses for
structured configs is only required during the initial serialization for this mode.

`Auto` - This mode will try to deserialize according to the Dataclass mode and fall back to the DictConfig mode if the
dataclass definition is not available. This is the default mode.

You can set the transformer mode globally or for the current context only the following ways:
```python
from flytekitplugins.omegaconf import set_transformer_mode, set_local_transformer_mode, OmegaConfTransformerMode

# Set the global transformer mode using the new function
set_transformer_mode(OmegaConfTransformerMode.DictConfig)

# You can also the mode for the current context only
with set_local_transformer_mode(OmegaConfTransformerMode.Dataclass):
    # This will use the Dataclass mode
    pass
```

```note
Since the DictConfig is flattened and keys transformed into dot notation, the keys of the DictConfig must not contain
dots.
```
