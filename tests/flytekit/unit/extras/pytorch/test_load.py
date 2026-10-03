from unittest import mock

import torch

from flytekit.extras.pytorch import PyTorchModuleTransformer, PyTorchTensorTransformer
from flytekit.extras.pytorch.native import load_torch_object


def test_load_torch_object_keeps_torch_default():
    with mock.patch("torch.load", autospec=True) as m:
        load_torch_object("model.pt", map_location="cpu")
    m.assert_called_once_with("model.pt", map_location="cpu")


def test_load_torch_object_passes_weights_only():
    with mock.patch("torch.load", autospec=True) as m:
        load_torch_object("model.pt", map_location="cpu", weights_only=False)
    m.assert_called_once_with("model.pt", map_location="cpu", weights_only=False)


def test_load_torch_object_omits_weights_only_for_old_torch():
    def legacy_load(f, map_location=None):
        return "loaded"

    with mock.patch("torch.load", new=legacy_load):
        assert load_torch_object("model.pt", map_location="cpu", weights_only=False) == "loaded"


def test_transformers_weights_only_setting():
    # tensors load fine with torch's safe default, whole modules do not
    assert PyTorchTensorTransformer.WEIGHTS_ONLY is None
    assert PyTorchModuleTransformer.WEIGHTS_ONLY is False


def test_module_round_trip_uses_full_unpickling(tmp_path):
    module = torch.nn.Sequential(torch.nn.Linear(2, 2), torch.nn.BatchNorm1d(2))
    path = str(tmp_path / "module.pt")
    torch.save(module, path)
    loaded = load_torch_object(path, map_location="cpu", weights_only=PyTorchModuleTransformer.WEIGHTS_ONLY)
    assert isinstance(loaded, torch.nn.Sequential)
