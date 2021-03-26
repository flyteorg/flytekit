import six as _six

from flytekit.common import promise as _promise
from flytekit.common import sdk_bases as _sdk_bases
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.types import containers as _containers
from flytekit.common.types import helpers as _type_helpers
from flytekit.common.types import primitives as _primitives
from flytekit.models import interface as _interface_models
from flytekit.models import literals as _literal_models


class BindingData(_literal_models.BindingData, metaclass=_sdk_bases.ExtendedSdkType):
    @staticmethod
    def _has_sub_bindings(m):
        """
        :param dict[Text,T] or list[T]:
        :rtype: bool
        """
        for v in _six.itervalues(m) if isinstance(m, dict) else m:
            if isinstance(v, (list, dict)) and BindingData._has_sub_bindings(v):
                return True
            elif isinstance(v, (_promise.Input, _promise.NodeOutput)):
                return True
        return False

    @classmethod
    def promote_from_model(cls, model):
        """
        :param flytekit.models.literals.BindingData model:
        :rtype: BindingData
        """
        return cls(
            scalar=model.scalar,
            collection=model.collection,
            promise=model.promise,
            map=model.map,
        )

    @classmethod
    def from_python_std(cls, literal_type, t_value, upstream_nodes=None):
        """
        :param flytekit.models.types.LiteralType literal_type:
        :param T t_value:
        :param list[flytekit.common.nodes.SdkNode] upstream_nodes: [Optional] Keeps track of the nodes upstream,
            if applicable.
        :rtype: BindingData
        """
        scalar = None
        collection = None
        promise = None
        map = None
        downstream_sdk_type = _type_helpers.get_sdk_type_from_literal_type(literal_type)
        if isinstance(t_value, _promise.Input):
            if not downstream_sdk_type.is_castable_from(t_value.sdk_type):
                _user_exceptions.FlyteTypeException(
                    t_value.sdk_type,
                    downstream_sdk_type,
                    additional_msg="When binding workflow input: {}".format(t_value),
                )
            promise = t_value.promise
        elif isinstance(t_value, _promise.NodeOutput):
            if not downstream_sdk_type.is_castable_from(t_value.sdk_type):
                _user_exceptions.FlyteTypeException(
                    t_value.sdk_type,
                    downstream_sdk_type,
                    additional_msg="When binding node output: {}".format(t_value),
                )
            promise = t_value
            if upstream_nodes is not None:
                upstream_nodes.append(t_value.sdk_node)
        elif isinstance(t_value, list):
            if not issubclass(downstream_sdk_type, _containers.ListImpl):
                raise _user_exceptions.FlyteTypeException(
                    type(t_value),
                    downstream_sdk_type,
                    received_value=t_value,
                    additional_msg="Cannot bind a list to a non-list type.",
                )
            collection = _literal_models.BindingDataCollection(
                [
                    BindingData.from_python_std(
                        downstream_sdk_type.sub_type.to_flyte_literal_type(),
                        v,
                        upstream_nodes=upstream_nodes,
                    )
                    for v in t_value
                ]
            )
        elif isinstance(t_value, dict) and (
            not issubclass(downstream_sdk_type, _primitives.Generic) or BindingData._has_sub_bindings(t_value)
        ):
            # TODO: This behavior should be embedded in the type engine.  Someone should be able to alter behavior of
            # TODO: binding logic by injecting their own type engine.  The same goes for the list check above.
            raise NotImplementedError("TODO: Cannot use map bindings at the moment")
        else:
            sdk_value = downstream_sdk_type.from_python_std(t_value)
            scalar = sdk_value.scalar
            collection = sdk_value.collection
            map = sdk_value.map
        return cls(scalar=scalar, collection=collection, map=map, promise=promise)


class TypedInterface(_interface_models.TypedInterface, metaclass=_sdk_bases.ExtendedSdkType):
    @classmethod
    def promote_from_model(cls, model):
        """
        :param flytekit.models.interface.TypedInterface model:
        :rtype: TypedInterface
        """
        return cls(model.inputs, model.outputs)

    def create_bindings_for_inputs(self, map_of_bindings):
        """
        :param dict[Text, T] map_of_bindings:  This can be scalar primitives, it can be node output references,
            lists, etc..
        :rtype: (list[flytekit.models.literals.Binding], list[flytekit.common.nodes.SdkNode])
        :raises: flytekit.common.exceptions.user.FlyteAssertion
        """
        binding_data = dict()
        all_upstream_nodes = list()
        for k in sorted(self.inputs):
            var = self.inputs[k]
            if k not in map_of_bindings:
                raise _user_exceptions.FlyteAssertion("Input was not specified for: {} of type {}".format(k, var.type))

            binding_data[k] = BindingData.from_python_std(
                var.type, map_of_bindings[k], upstream_nodes=all_upstream_nodes
            )

        extra_inputs = set(binding_data.keys()) ^ set(map_of_bindings.keys())
        if len(extra_inputs) > 0:
            raise _user_exceptions.FlyteAssertion(
                "Too many inputs were specified for the interface.  Extra inputs were: {}".format(extra_inputs)
            )

        seen_nodes = set()
        min_upstream = list()
        for n in all_upstream_nodes:
            if n not in seen_nodes:
                seen_nodes.add(n)
                min_upstream.append(n)

        return (
            [_literal_models.Binding(k, bd) for k, bd in _six.iteritems(binding_data)],
            min_upstream,
        )

    def __repr__(self):
        return "({inputs}) -> ({outputs})".format(
            inputs=", ".join(
                [
                    "{}: {}".format(k, _type_helpers.get_sdk_type_from_literal_type(v.type))
                    for k, v in _six.iteritems(self.inputs)
                ]
            ),
            outputs=", ".join(
                [
                    "{}: {}".format(k, _type_helpers.get_sdk_type_from_literal_type(v.type))
                    for k, v in _six.iteritems(self.outputs)
                ]
            ),
        )
