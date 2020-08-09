from __future__ import absolute_import

import six as _six

from flytekit.common import sdk_bases as _sdk_bases
from flytekit.common.mixins import registerable as _registerable


class ExampleRegisterable(_six.with_metaclass(_sdk_bases.ExtendedSdkType,
                                              _registerable.RegisterableEntity,
                                              _registerable.LocallyDefined)):
    def __init__(self, *args, **kwargs):
        super(ExampleRegisterable, self).__init__(*args, **kwargs)

    def promote_from_model(cls, base_model):
        pass


example = ExampleRegisterable()
