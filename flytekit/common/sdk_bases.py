from __future__ import absolute_import
from flytekit.models import common as _common
import abc as _abc
import six as _six


class ExtendedSdkType(_six.with_metaclass(_common.FlyteABCMeta, _common.FlyteType)):
    """
    Abstract class that all SDK objects must inherit from.  This provides the ability to promote a data model object
    into an actionable object.
    """

    @_abc.abstractmethod
    def promote_from_model(cls, base_model):
        """
        :param flytekit.models.common.FlyteIdlEntity base_model:
        :rtype: ExtendedSdkType
        """
        pass

    def from_flyte_idl(cls, pb2_object):
        base_model = super(ExtendedSdkType, cls).from_flyte_idl(pb2_object)
        return cls.promote_from_model(base_model)
