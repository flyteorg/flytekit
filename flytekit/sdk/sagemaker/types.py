import enum


class HyperparameterScalingType(enum.Enum):
    AUTO = 0
    LINEAR = 1
    LOGARITHMIC = 2
    REVERSELOGARITHMIC = 3


class HyperparameterTuningObjectiveType(enum.Enum):
    MINIMIZE = 0
    MAXIMIZE = 1


class InputMode(enum.Enum):
    FILE = 0
    PIPE = 1


class AlgorithmName(enum.Enum):
    CUSTOM = 0
    XGBOOST = 1