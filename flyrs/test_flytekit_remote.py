import timeit

from flytekit.configuration import Config
from flytekit.remote import FlyteRemote

PROJECT = "flytesnacks"
DOMAIN = "development"

remote_py = FlyteRemote(Config.auto(), default_project=PROJECT, default_domain=DOMAIN)
task_py = remote_py.fetch_task(
    project=PROJECT, domain=DOMAIN, name="workflows_.say_1", version="WhIAnhpyjrAdaRvrQ9Cjpw"
)
# print(task_py)

remote_rs = FlyteRemote(Config.auto(), enable_rs=True, default_project=PROJECT, default_domain=DOMAIN)
task_rs = remote_rs.fetch_task(
    project=PROJECT, domain=DOMAIN, name="workflows_.say_1", version="WhIAnhpyjrAdaRvrQ9Cjpw"
)
# print(task_rs)

print(task_py == task_rs)


setup = """
from flytekit.remote import FlyteRemote;
from flytekit.configuration import Config;
PROJECT = "flytesnacks";
DOMAIN = "development";
remote_py = FlyteRemote(Config.auto(), default_project=PROJECT, default_domain=DOMAIN);
remote_rs = FlyteRemote(Config.auto(), enable_rs=True, default_project=PROJECT, default_domain=DOMAIN);
"""

fetch_task_in_py = """task_py = remote_py.fetch_task(project=PROJECT, domain=DOMAIN, name="workflows_.say_1", version="WhIAnhpyjrAdaRvrQ9Cjpw")"""
fetch_task_in_rs = """task_rs = remote_rs.fetch_task(project=PROJECT, domain=DOMAIN, name="workflows_.say_1", version="WhIAnhpyjrAdaRvrQ9Cjpw")"""
# Python gRPC
print(sum(timeit.repeat(fetch_task_in_py, setup=setup, repeat=10, number=100)))
# Rust gRPC
print(sum(timeit.repeat(fetch_task_in_rs, setup=setup, repeat=10, number=100)))
