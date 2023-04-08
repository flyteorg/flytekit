from dataclasses import dataclass

import torch
from dataclasses_json import dataclass_json
from flytekitplugins.torchelastic.task import Elastic

from flytekit import task, workflow


@dataclass_json
@dataclass
class Config:
    lr: float = 1e-5
    bs: int = 64
    name: str = "foo"


@task
def init_model() -> torch.nn.Module:
    model = torch.nn.Linear(11, 22)

    return model


@task(task_config=Elastic())
def train(config: Config, model: torch.nn.Module) -> tuple[str, Config, torch.nn.Module]:
    import os

    import torch

    local_rank = os.environ["LOCAL_RANK"]

    out_model = torch.nn.Linear(1000, int(local_rank) + 1)
    
    config.name = "modified"
    return f"result from local rank {local_rank}", config, out_model


@workflow
def wf(config: Config = Config()) -> tuple[str, Config, torch.nn.Module]:
    model = init_model()
    return train(config=config, model=model)


def test_end_to_end():
    r, cfg, m = wf()
    assert "result from local rank 0" in r
    assert cfg.name == "modified"
    assert m.in_features == 1000
    assert m.out_features == 1
