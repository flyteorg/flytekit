from typing import Any, Dict, List, Tuple

from flytekit.models import interface as _interface_models
from flytekit.models import literals as _literal_models
from flytekit.remote import nodes as _nodes


class TypedInterface(_interface_models.TypedInterface):
    @classmethod
    def promote_from_model(cls, model):
        """
        :param flytekit.models.interface.TypedInterface model:
        :rtype: TypedInterface
        """
        return cls(model.inputs, model.outputs)

    def create_bindings_for_inputs(
        self, map_of_bindings: Dict[str, Any]
    ) -> Tuple[List[_literal_models.Binding], List[_nodes.FlyteNode]]:
        """
        :param: map_of_bindings: this can be scalar primitives, it can be node output references, lists, etc.
        :raises: flytekit.common.exceptions.user.FlyteAssertion
        """
        return [], []
