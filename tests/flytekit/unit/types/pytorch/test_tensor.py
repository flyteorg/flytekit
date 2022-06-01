import torch

from flytekit import task, workflow


@task
def generate_tensor_1d() -> torch.Tensor:
    return torch.zeros(5, dtype=torch.int32)


@task
def generate_tensor_2d() -> torch.Tensor:
    return torch.tensor([[1.0, -1.0, 2], [1.0, -1.0, 9], [0, 7.0, 3]])


@task
def t1(tensor: torch.Tensor) -> torch.Tensor:
    assert tensor.dtype == torch.int32
    tensor[0] = 1
    return tensor


@task
def t2(tensor: torch.Tensor) -> torch.Tensor:
    # convert 2D to 3D
    tensor.unsqueeze_(-1)
    return tensor.expand(3, 3, 2)


@workflow
def wf():
    t1(tensor=generate_tensor_1d())
    t2(tensor=generate_tensor_2d())


@workflow
def test_wf():
    wf()
