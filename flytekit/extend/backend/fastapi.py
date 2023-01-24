import typing
from http import HTTPStatus

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from flytekit import __version__
from flytekit.extend.backend.base_plugin import (
    BackendPluginBase,
    BackendPluginRegistry,
    CreateRequest,
    CreateResponse,
    PollRequest,
    PollResponse,
)

PLUGINS_V1 = "plugins/v1"


def _create_root_welcome(app: FastAPI, plugins: typing.List[BackendPluginBase]):
    l = ""
    for p in plugins:
        l += f"<li>TaskType: {p.task_type}, Version: {__version__}</li>"

    @app.get("/", response_class=HTMLResponse)
    def root():
        return f"""
            <html>
                <head>
                    <title>FlyteBackend Plugin Server</title>
                </head>
                <body>
                    <h1>Flyte Backend plugin server.</h1>
                    <h2>Registered plugins<h2>
                    <ul>
                        {l}
                    </ul>
                </body>
            </html>
        """


def _create_health_check(app: FastAPI):
    @app.get("/health")
    def health():
        return {"message": HTTPStatus.OK.phrase, "status": HTTPStatus.OK}


def _serve_plugin(app: FastAPI, plugin: BackendPluginBase):
    @app.post(f"/{PLUGINS_V1}/{plugin.task_type}", response_model=CreateResponse)
    async def create(create_request: CreateRequest):
        return await plugin.create(create_request)

    @app.delete(f"/{PLUGINS_V1}/{plugin.task_type}")
    async def terminate(job_id: str):
        return await plugin.terminate(job_id)

    @app.get(f"/{PLUGINS_V1}/{plugin.task_type}", response_model=PollResponse)
    async def poll(poll_request: PollRequest):
        return await plugin.poll(poll_request)


def serve_all_registered_plugins(app: FastAPI):
    plugins = BackendPluginRegistry.list_registered_plugins()
    _create_root_welcome(app, plugins)
    _create_health_check(app)
    for plugin in plugins:
        plugin.initialize()
        _serve_plugin(app, plugin)


app = FastAPI()
serve_all_registered_plugins(app)
