import typing
from http import HTTPStatus

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from flytekit.extend.backend.plugin import BackendPluginRegistry, BackendPluginBase


def _create_root_welcome(app: FastAPI, plugins: typing.List[BackendPluginBase]):
    l = ""
    for p in plugins:
        l += f"<li>ID: {p.identifier}, TaskType: {p.task_type}, Version: {p.version}</li>"

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


def _create_healthcheck(app: FastAPI):
    @app.get("/health")
    def health():
        return {"message": HTTPStatus.OK.phrase, "status": HTTPStatus.OK}


def _serve_plugin(app: FastAPI, plugin: BackendPluginBase):
    @app.post(f"/plugins/v1/{plugin.identifier}/{plugin.version}/")
    async def create():
        return await plugin.create()

    @app.delete(f"/plugins/v1/{plugin.identifier}/{plugin.version}/")
    async def delete():
        return await plugin.terminate()

    @app.get(f"/plugins/v1/{plugin.identifier}/{plugin.version}/")
    async def poll():
        return await plugin.poll()


def serve_plugin(app: FastAPI, plugin: BackendPluginBase):
    _create_root_welcome(app, [plugin])
    _create_healthcheck(app)
    _serve_plugin(app, plugin)


def serve_all_registered_plugins(app: FastAPI):
    plugins = BackendPluginRegistry.list_registered_plugins()
    _create_root_welcome(app, plugins)
    _create_healthcheck(app)
    for plugin in plugins:
        _serve_plugin(app, plugin)


app = FastAPI()
serve_all_registered_plugins(app)