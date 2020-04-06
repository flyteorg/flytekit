from __future__ import absolute_import

from flytekit.common.mixins import registerable as _registerable
from flytekit.common import interface as _interface, nodes as _nodes, sdk_bases as _sdk_bases
import six as _six


class ExampleRegisterable(_six.with_metaclass(_sdk_bases.ExtendedSdkType, _registerable.RegisterableEntity)):
    def __init__(self, *args, **kwargs):
        super(ExampleRegisterable, self).__init__(*args, **kwargs)

    def promote_from_model(cls, base_model):
        pass


example = ExampleRegisterable()
