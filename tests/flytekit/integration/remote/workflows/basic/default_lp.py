import datetime

from flytekit import task, workflow


@task
def print_datetime(time: datetime.datetime):
    print(time)


@workflow
def my_wf(time: datetime.datetime = datetime.datetime.now()):
    print_datetime(time=time)


if __name__ == "__main__":
    print(f"Running my_wf() {my_wf()}")
