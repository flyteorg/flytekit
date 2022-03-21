import textwrap
import typing

from flytekit.models import interface as _interface_models
from flytekit.models.interface import Variable


class TypedInterface(_interface_models.TypedInterface):
    @classmethod
    def promote_from_model(cls, model):
        """
        :param flytekit.models.interface.TypedInterface model:
        :rtype: TypedInterface
        """
        return cls(model.inputs, model.outputs)
