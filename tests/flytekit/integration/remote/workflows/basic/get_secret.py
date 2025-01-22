from flytekit import task, current_context, Secret, workflow

secret = Secret(
    group="my-group",
    key="token",
)


@task(secret_requests=[secret])
def get_secret() -> str:
    return current_context().secrets.get(group="my-group", key="token")


@workflow
def wf() -> str:
    return get_secret()
