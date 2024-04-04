import typing as t
from dataclasses import dataclass, field


@dataclass
class ExampleNestedConfig:
    nested_int_key: int = 2


@dataclass
class ExampleConfig:
    int_key: int = 1337
    union_key: t.Union[int, str] = 1337
    any_key: t.Any = "1337"
    optional_key: t.Optional[int] = 1337
    dictconfig_key: ExampleNestedConfig = ExampleNestedConfig()
    optional_dictconfig_key: t.Optional[ExampleNestedConfig] = None
    listconfig_key: t.List[int] = field(default_factory=lambda: (1, 2, 3))
    tuple_int_key: t.Tuple[int, int] = field(default_factory=lambda: (1, 2))


@dataclass
class ExampleConfigWithNonAnnotatedSubtree:
    unnanotated_key = 1
    annotated_key: ExampleNestedConfig = ExampleNestedConfig()
