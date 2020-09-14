from flytekit.common import sdk_bases as _sdk_bases
from flytekit.common.mixins import registerable as _registerable


class ExampleRegisterable(
    _registerable.RegisterableEntity, _registerable.TrackableEntity, metaclass=_sdk_bases.ExtendedSdkType
):
    def __init__(self, *args, **kwargs):
        super(ExampleRegisterable, self).__init__(*args, **kwargs)

    def promote_from_model(cls, base_model):
        pass


example = ExampleRegisterable()
