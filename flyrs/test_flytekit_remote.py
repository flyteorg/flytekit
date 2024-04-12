from flytekit.configuration import Config
from flytekit.remote import FlyteRemote

PROJECT = "flytesnacks"
DOMAIN = "development"

remote_py = FlyteRemote(Config.auto(), default_project=PROJECT, default_domain=DOMAIN)
task_py = remote_py.fetch_task(
    project=PROJECT, domain=DOMAIN, name="workflows_.say_1", version="WhIAnhpyjrAdaRvrQ9Cjpw"
)
print(task_py)

remote_rs = FlyteRemote(Config.auto(), enable_rs=True, default_project=PROJECT, default_domain=DOMAIN)
task_rs = remote_rs.fetch_task(
    project=PROJECT, domain=DOMAIN, name="workflows_.say_1", version="WhIAnhpyjrAdaRvrQ9Cjpw"
)
print(task_rs)

assert task_py == task_rs