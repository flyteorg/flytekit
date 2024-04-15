from flytekit.configuration import Config
from flytekit.remote import FlyteRemote
from remote.remote import RustFlyteRemote

PROJECT = "flytesnacks"
DOMAIN = "development"

remote_py = FlyteRemote(Config.auto(), default_project=PROJECT, default_domain=DOMAIN)
task_py = remote_py.fetch_task(
     project=PROJECT, domain=DOMAIN, name="workflows_.say_1", version="WhIAnhpyjrAdaRvrQ9Cjpw"
 )
# print(task_py)

remote_rs = RustFlyteRemote(Config.auto(), default_project=PROJECT, default_domain=DOMAIN)
task_rs = remote_rs.fetch_task(
    project=PROJECT, domain=DOMAIN, name="workflows_.say_1", version="WhIAnhpyjrAdaRvrQ9Cjpw"
)
# print(task_rs)
assert task_py == task_rs


tasks_py = remote_py.list_tasks_by_version(
     project=PROJECT, domain=DOMAIN, version="WhIAnhpyjrAdaRvrQ9Cjpw"
)
tasks_rs = remote_rs.list_tasks_by_version(
    project=PROJECT, domain=DOMAIN, version="WhIAnhpyjrAdaRvrQ9Cjpw"
)
assert tasks_py == tasks_rs

