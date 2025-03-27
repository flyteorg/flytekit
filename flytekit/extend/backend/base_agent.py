# These classes are deprecated and will be removed in the future.
from .base_connector import AsyncConnectorBase as AsyncAgentBase  # noqa: F401
from .base_connector import AsyncConnectorExecutorMixin as AsyncAgentExecutorMixin  # noqa: F401
from .base_connector import ConnectorBase as AgentBase  # noqa: F401
from .base_connector import ConnectorRegistry as AgentRegistry  # noqa: F401
from .base_connector import Resource, ResourceMeta, TaskCategory  # noqa: F401
from .base_connector import SyncConnectorBase as SyncAgentBase  # noqa: F401
from .base_connector import SyncConnectorExecutorMixin as SyncAgentExecutorMixin  # noqa: F401
