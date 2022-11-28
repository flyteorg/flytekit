import typing

from flytekit import FlyteContext
from flytekit.models.core.identifier import Identifier
from flytekit.remote.remote_callable import RemoteEntity

T = typing.TypeVar("T")


class LazyEntity(RemoteEntity, typing.Generic[T]):
    """
    Fetches the entity when the entity is called or when the entity is retrieved.
    The entity is derived from RemoteEntity so that it behaves exactly like the mimiced entity.
    """

    def __init__(self, id: Identifier, getter: typing.Callable[[], T], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._entity = None
        self._getter = getter
        self._identifier = id

    @property
    def name(self) -> str:
        return self._identifier.name

    def entity_fetched(self) -> bool:
        return self._entity is not None

    @property
    def entity(self) -> T:
        """
        If not already fetched / available, then the entity will be force fetched.
        """
        if self._entity is None:
            self._entity = self._getter()
        return self._entity

    def __getattr__(self, item: str) -> typing.Any:
        """
        Forwards all other attributes to entity, causing the entity to be fetched!
        """
        return getattr(self.entity, item)

    @property
    def identifier(self) -> Identifier:
        """
        Returns the identifier for the entity that will be fetched
        """
        return self._identifier

    def compile(self, ctx: FlyteContext, *args, **kwargs):
        return self.entity.compile(ctx, *args, **kwargs)

    def __call__(self, *args, **kwargs):
        """
        Forwards the call to the underlying entity. The entity will be fetched if not already present
        """
        return self.entity(*args, **kwargs)

    def __repr__(self) -> str:
        return str(self)

    def __str__(self) -> str:
        return f"Promise for entity [{self._identifier}]"
