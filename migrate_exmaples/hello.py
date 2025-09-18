import logging

from flytekit import ImageSpec, dynamic, task, workflow

image = ImageSpec(apt_packages=["vim"], packages=["pandas"])


@task(cache=True, cache_version="1.0", retries=3, container_image=image)
def say_hello(name: str):
    print(f"Hello, {name}!")


@dynamic(container_image=image)
def dynamic_task(name: str):
    say_hello(name=name)


@workflow
def wf(name: str):
    say_hello(name=name)
    dynamic_task(name=name)


if __name__ == "__main__":
    import flyte
    flyte.init_from_config(log_level=logging.DEBUG)
    # run = flyte.with_runcontext(log_level=logging.DEBUG).run(wf, name="flyte")
    # print(run.name)
    # print(run.url)
