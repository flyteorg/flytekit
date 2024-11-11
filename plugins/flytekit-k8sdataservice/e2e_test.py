import os
from flytekitplugins.k8sdataservice import DataServiceConfig, DataServiceTask, CleanupSensor
from flytekit import kwtypes, Resources, task, workflow


def graph_data_service():
    gnn_config = DataServiceConfig(
        Name="test_new",
        Requests=Resources(cpu='1', mem='1Gi'),
        Limits=Resources(cpu='2', mem='2Gi'),
        Replicas=1,
        Image="python:3.10-slim-bookworm",
        ProxyAs='test',
        Command=[
            "bash",
            "-c",
            "sleep 15m",
        ],
        Cluster="grid2",
    )
    gnn_task = DataServiceTask(
        name="Graph Data Service",
        inputs=kwtypes(ds=str),
        task_config=gnn_config,
    )
    return gnn_task


# @task(
#     container_image=custom_config.get_job_config_image(),
#     proxy_as=custom_config.get_proxy_as(),
#     pod_template=custom_config.get_custom_pod_template(),
#     instance_type="a100_1",
#     environment=custom_config.get_environment_config(),
#     task_config=HorovodJob(
#         slots=1,
#         launcher=Launcher(
#             requests=Resources(cpu="1", mem="5Gi"),
#             limits=Resources(cpu="5", mem="10Gi"),
#             replicas=custom_config.get_job_config_replicas('training', 'launcher'),
#             command=custom_config.get_command('training', 'launcher'),
#         ),
#         worker=Worker(
#             replicas=custom_config.get_job_config_replicas('training', 'worker'),
#             command=custom_config.get_command('training', 'worker'),
#         )
#     )
# )
# def GNN_Training():
#     pass


gnn_sensor = CleanupSensor(name="Cleanup")


@workflow
def test_dataservice_wf():
    graph_data_service()(ds="demo Ads 3X") >> gnn_sensor(
        release_name="test_new",
        cleanup_data_service=False,
        cluster='grid2')
    

if __name__ == "__main__":
    print(f"Running test_dataservice_wf() {test_dataservice_wf()}")