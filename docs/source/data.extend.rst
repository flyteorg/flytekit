##############################
Extend Data Persistence layer
##############################
Flytekit provides a data persistence layer, which is used for recording metadata that is shared with backend Flyte. This persistence layer is also available for various types to store raw user data and is designed to be cross-cloud compatible.
Moreover, it is design to be extensible and users can bring their own data persistence plugins by following the persistence interface. NOTE, this is bound to get more extensive for variety of use-cases, but the core set of apis are battle tested.

.. automodule:: flytekit.core.data_persistence
    :no-members:
    :no-inherited-members:
    :no-special-members:

.. automodule:: flytekit.extras.persistence
    :no-members:
    :no-inherited-members:
    :no-special-members:
