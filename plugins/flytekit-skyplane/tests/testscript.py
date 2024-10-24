from flytekit import workflow, task
# from flytekitplugin.skyplane import
from flytekitplugin.skyplane.skyplane import SkyplaneFunctionTask, SkyplaneJob

@task
def dummy_data_transfer_task():
    job_config = SkyplaneJob(
        source="s3://my-source-bucket/data/",
        destination="s3://my-destination-bucket/data/",
        options={"overwrite": "true"}
    )
    skyplane_task = SkyplaneFunctionTask(job_config=job_config, task_function=lambda: "Data transferred!")
    return skyplane_task

@workflow
def test_workflow():
    return dummy_data_transfer_task()

if __name__ == "__main__":
    print(test_workflow())