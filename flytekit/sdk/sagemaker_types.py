import enum


class HyperparameterScalingType(enum.Enum):
    AUTO = 0
    LINEAR = 1
    LOGARITHMIC = 2
    REVERSELOGARITHMIC = 3


class HPOJobObjectiveType(enum.Enum):
    MINIMIZE = 0
    MAXIMIZE = 1
