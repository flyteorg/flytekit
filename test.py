from flytekit import task, workflow
from flytekitplugins.flyin import vscode, VscodeConfig, DEFAULT_CODE_SERVER_EXTENSIONS

@task(
    container_image="localhost:30000/flytekit-cache:0.0.1",
    environment={"FLYTE_SDK_LOGGING_LEVEL": "20"}
)
@vscode(

)
def t():
    ...



# this vscode task will be killed within 10 secs
@task(
    container_image="localhost:30000/flytekit-cache:0.0.1",
    environment={"FLYTE_SDK_LOGGING_LEVEL": "20"}
)
@vscode(
    max_idle_seconds=10,
)
def t_short_live():
    ...



# this vscode task will download default extension + vim extension
config_with_vim = VscodeConfig(
    extension_remote_paths=DEFAULT_CODE_SERVER_EXTENSIONS+["https://open-vsx.org/api/vscodevim/vim/1.27.0/file/vscodevim.vim-1.27.0.vsix"]
)

@task(
    container_image="localhost:30000/flytekit-cache:0.0.1",
    environment={"FLYTE_SDK_LOGGING_LEVEL": "20"}
)
@vscode(
    config=config_with_vim
)
def t_vim():
    ...



@workflow
def wf():
    t()
    t_short_live()
    t_vim()