from flytekit.extras.pod_templates import attach_shm
from flytekit.core.task import task

def test_attach_shm():

    shm = attach_shm("SHM", "5Gi")

    @task(pod_template=shm)
    def my_task():
        pass
