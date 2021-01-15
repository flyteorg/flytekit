from dataclasses import dataclass


@dataclass
class Resources(object):
    cpu: str = None
    mem: str = None
    gpu: str = None
    storage: str = None


@dataclass
class ResourceSpec(object):
    requests: Resources = None
    limits: Resources = None
