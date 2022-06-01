import torch

from flytekit import task, workflow
from flytekit.types.pytorch import PyTorchStateDict


@task
def generate_module() -> PyTorchStateDict:
    bn = torch.nn.BatchNorm1d(3, track_running_stats=True)
    return PyTorchStateDict(module=bn)


@task
def t1(state_dict: PyTorchStateDict):
    new_bn = torch.nn.BatchNorm1d(3, track_running_stats=True)
    new_bn.load_state_dict(state_dict)


class MyModel(torch.nn.Module):
    def __init__(self):
        super(MyModel, self).__init__()
        self.l0 = torch.nn.Linear(4, 2)
        self.l1 = torch.nn.Linear(2, 1)

    def forward(self, input):
        out0 = self.l0(input)
        out0_relu = torch.nn.functional.relu(out0)
        return self.l1(out0_relu)


@task
def generate_model() -> PyTorchStateDict:
    bn = MyModel()
    return PyTorchStateDict(module=bn)


@task
def t2(state_dict: PyTorchStateDict):
    new_bn = MyModel()
    new_bn.load_state_dict(state_dict)


@workflow
def wf():
    state_dict_1 = generate_module()
    t1(state_dict=state_dict_1)
    state_dict_2 = generate_model()
    t2(state_dict=state_dict_2)
    t2(state_dict=PyTorchStateDict(module=MyModel()))


@workflow
def test_wf():
    wf()
