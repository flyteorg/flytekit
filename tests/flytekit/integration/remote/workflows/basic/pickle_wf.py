import typing

from flytekit import task, workflow


@task
def t1(game_id: typing.Any):
    print(game_id)


# test
@workflow
def pickle_wf():
    t1(game_id="123456")
