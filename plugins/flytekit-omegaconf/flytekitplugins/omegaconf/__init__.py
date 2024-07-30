from contextlib import contextmanager

from flytekitplugins.omegaconf.config import OmegaConfTransformerMode
from flytekitplugins.omegaconf.dictconfig_transformer import DictConfigTransformer  # noqa: F401
from flytekitplugins.omegaconf.listconfig_transformer import ListConfigTransformer  # noqa: F401

_TRANSFORMER_MODE = OmegaConfTransformerMode.Auto


def set_transformer_mode(mode: OmegaConfTransformerMode) -> None:
    """Set the global serialization mode for OmegaConf objects."""
    global _TRANSFORMER_MODE
    _TRANSFORMER_MODE = mode


def get_transformer_mode() -> OmegaConfTransformerMode:
    """Get the global serialization mode for OmegaConf objects."""
    return _TRANSFORMER_MODE


@contextmanager
def local_transformer_mode(mode: OmegaConfTransformerMode):
    """Context manager to set a local serialization mode for OmegaConf objects."""
    global _TRANSFORMER_MODE
    previous_mode = _TRANSFORMER_MODE
    set_transformer_mode(mode)
    try:
        yield
    finally:
        set_transformer_mode(previous_mode)


__all__ = ["set_transformer_mode", "get_transformer_mode", "local_transformer_mode", "OmegaConfTransformerMode"]
