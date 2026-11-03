from dataclasses import dataclass
from typing import Optional

from flytekit import task, workflow
from mashumaro.mixins.json import DataClassJSONMixin


@dataclass
class MyConfig(DataClassJSONMixin):
    op_list: Optional[list[str]] = None


@task
def t1(config: MyConfig) -> str:
    if config.op_list:
        return ",".join(config.op_list)
    return ""


@workflow
def wf(config: MyConfig = MyConfig()) -> str:
    return t1(config=config)


if __name__ == "__main__":
    wf()
