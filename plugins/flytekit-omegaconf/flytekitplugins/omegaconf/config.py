from enum import Enum


class OmegaConfTransformerMode(Enum):
    """
    Operation Mode indicating whether a (potentially unannotated) DictConfig object or a structured config using the
    underlying dataclass is returned.

    Note: We define a single shared configs across all transformers as recursive calls should refer to the same config
    Note: The latter requires the use of structured configs.
    """

    DictConfig = "DictConfig"
    DataClass = "DataClass"
    Auto = "Auto"


class SharedConfig:
    _mode: OmegaConfTransformerMode = OmegaConfTransformerMode.Auto

    @classmethod
    def get_mode(cls) -> OmegaConfTransformerMode:
        """Get the current mode for serialising omegaconf objects."""
        return cls._mode

    @classmethod
    def set_mode(cls, new_mode: OmegaConfTransformerMode) -> None:
        """Set the current mode for serialising omegaconf objects."""
        if isinstance(new_mode, OmegaConfTransformerMode):
            cls._mode = new_mode
