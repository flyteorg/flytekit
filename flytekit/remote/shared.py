from flytekit.clients.friendly import SynchronousFlyteClient


class RemoteClient:
    """
    Mixin for a remote object which has a 'client' property.
    """

    @property
    def client(self) -> SynchronousFlyteClient:
        pass
