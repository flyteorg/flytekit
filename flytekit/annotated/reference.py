from flytekit.models.core import identifier as _identifier_model


class Reference(object):
    def __init__(
            self, type: int, project: str, domain: str, name: str, version: str, *args, **kwargs,
    ):
        self._id = _identifier_model.Identifier(_identifier_model.ResourceType.TASK, project, domain, name, version)

    @property
    def id(self) -> _identifier_model.Identifier:
        return self._id


class TaskReference(Reference):
    def __init__(
            self, project: str, domain: str, name: str, version: str, *args, **kwargs,
    ):
        super().__init__(_identifier_model.ResourceType.TASK, project, domain, name, version, *args, **kwargs)
