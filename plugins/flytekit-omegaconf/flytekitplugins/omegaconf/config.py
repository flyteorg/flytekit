from enum import Enum


class OmegaConfTransformerMode(Enum):
    """
    Operation Mode indicating whether a (potentially unannotated) DictConfig object or a structured config using the
    underlying dataclass is returned.

    Note: We define a single shared config across all transformers as recursive calls should refer to the same config
    Note: The latter requires the use of structured configs.
    """

    DictConfig = "DictConfig"
    DataClass = "DataClass"
    Auto = "Auto"
