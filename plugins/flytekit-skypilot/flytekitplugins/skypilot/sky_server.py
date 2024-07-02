from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from multiprocessing import Process, Manager
import time
import asyncio
from flytekitplugins.skypilot.utils import ClusterRegistry, MinimalRemoteSetting, TaskRemotePathSetting
from flytekitplugins.skypilot.process_utils import ConcurrentProcessHandler
from typing import Optional
import multiprocessing as mp
import sky
import uvicorn

cluster_registry = ClusterRegistry()
app = FastAPI()
_hostname: str = sky.utils.common_utils.get_user_hash()

# Define the request model
class LaunchModel(BaseModel):
    task: dict
    setting: Optional[MinimalRemoteSetting] = None
    
class SleepModel(BaseModel):
    seconds: int
    missing: Optional[MinimalRemoteSetting] = None
    class Config:
        arbitrary_types_allowed = True

# Create a dictionary to store process status
manager = Manager()
process_status = manager.dict()

@app.post("/launch")
async def launch(request: LaunchModel, background_tasks: BackgroundTasks):
    task: sky.Task = sky.Task.from_yaml_config(request.task)
    task_setting = TaskRemotePathSetting.from_minimal_setting(request.setting)
    new_loop = asyncio.get_event_loop()
    asyncio.run_coroutine_threadsafe(cluster_registry.create(task, task_setting), new_loop)
    # background_tasks.add_task(cluster_registry.create, task, task_setting)
    return {"hostname": _hostname}
    
@app.post("/stop_cluster")
async def stop_cluster(task_setting: MinimalRemoteSetting, background_tasks: BackgroundTasks):
    background_tasks.add_task(cluster_registry.stop_cluster, task_setting)
    return {"hostname": _hostname}
    
    
@app.get("/hostname")
def get_hostname():
    return {"hostname": _hostname}
    


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8787)
