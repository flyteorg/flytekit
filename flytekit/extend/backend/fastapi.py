import typing

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


def _serve_plugin(app: FastAPI, plugin: BackendPluginBase):
    @app.post("/plugins/")
    def create():
        plugin.create()


def serve_plugin(app: FastAPI, plugin: BackendPluginBase):
    _create_root_welcome(app, [plugin])
    _serve_plugin(app, plugin)


def serve_all_registered_plugins(app: FastAPI):

    for plugin in BackendPluginRegistry.list_registered_plugins():
        _serve_plugin(app, plugin)
