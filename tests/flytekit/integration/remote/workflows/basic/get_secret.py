from flytekit import task, Secret, workflow
from os import getenv

secret = Secret(
    group="my-group",
    key="token",
    env_var="MY_SECRET"
)


@task(secret_requests=[secret])
def get_secret() -> str:
    return getenv("MY_SECRET")


@workflow
def wf() -> str:
    return get_secret()
