import asyncio

import grpc

from flytekit.extend.backend.base_agent import AgentBase


def agent_error_handler(agent: AgentBase, context: grpc.ServicerContext, resource_meta: bytes):
    if agent.asynchronous:
        asyncio.run(agent.async_delete(context, resource_meta))
    else:
        agent.delete(context, resource_meta)
