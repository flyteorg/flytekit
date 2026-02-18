from flytekit.extras.pod_templates import attach_shm
from flytekit.core.task import task

def test_attach_shm():
 
    shm = attach_shm("SHM", "5Gi")
    assert shm.name == "SHM"
    assert shm.size == "5Gi"
 
    def my_task():
        pass

    # Verify pod template is attached to task
    assert my_task.pod_template == shm
