from flytekit.configuration import Config
from flytekit.remote import FlyteRemote
from remote.remote import RustFlyteRemote

PROJECT = "flytesnacks"
DOMAIN = "development"

TASK_NAME = "t.say_hi"
WF_NAME = "t.say_hi_wf"
VERSION_ID = "kQYNrRsnGenYk-Y2EF-y6A"

remote_py = FlyteRemote(Config.auto(), default_project=PROJECT, default_domain=DOMAIN)
remote_rs = RustFlyteRemote(Config.auto(), default_project=PROJECT, default_domain=DOMAIN)

task_py = remote_py.fetch_task(
     project=PROJECT, domain=DOMAIN, name=TASK_NAME, version=VERSION_ID
)
task_rs = remote_rs.fetch_task(
    project=PROJECT, domain=DOMAIN, name=TASK_NAME, version=VERSION_ID
)
assert task_py == task_rs

tasks_py = remote_py.list_tasks_by_version(
     project=PROJECT, domain=DOMAIN, version=VERSION_ID
)
tasks_rs = remote_rs.list_tasks_by_version(
    project=PROJECT, domain=DOMAIN, version=VERSION_ID
)
assert tasks_py == tasks_rs

workflow_py = remote_py.fetch_workflow(
     project=PROJECT, domain=DOMAIN, name=WF_NAME, version=VERSION_ID
 )
workflow_rs = remote_rs.fetch_workflow(
    project=PROJECT, domain=DOMAIN, name=WF_NAME, version=VERSION_ID
)
assert workflow_py == workflow_rs

