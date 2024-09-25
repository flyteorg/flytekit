import subprocess
from dataclasses import dataclass
from typing import Optional, Union, List

from mashumaro.mixins.json import DataClassJSONMixin
from mashumaro.mixins.yaml import DataClassYAMLMixin

from flytekit import ImageSpec, Resources, PythonFunctionTask, Secret
from flytekit.configuration import SerializationSettings, ImageConfig
from flytekit.core.base_task import TaskResolverMixin, Task
from flytekit.extras.accelerators import GPUAccelerator
from flytekit.remote import FlyteRemote


class ImageSpecConfig(DataClassYAMLMixin, DataClassJSONMixin, ImageSpec):
    def __hash__(self):
        return hash(self.to_dict().__str__())


@dataclass
class ResourceConfig(DataClassYAMLMixin, Resources):
    accelerator: Optional[str] = None


class SecretsConfig(DataClassYAMLMixin, DataClassJSONMixin, Secret):
    pass


@dataclass
class TaskLaunchConfig(DataClassYAMLMixin, DataClassJSONMixin):
    name: Optional[str] = None
    project: Optional[str] = None
    domain: Optional[str] = None
    run_command: Optional[str] = None
    container_image: Optional[Union[ImageSpecConfig, str]] = None
    resources: Optional[ResourceConfig] = None
    secrets: Optional[List[Secret]] = None
    copy_all: bool = False


def runner(script_file: str, run_command: str):
    if not run_command:
        run_command = ["python", script_file]
    print(f"Running {run_command}")
    shell = False if run_command and isinstance(run_command, list) else True
    subprocess.check_call(run_command, shell=shell)


class RawTaskLaunchResolver(TaskResolverMixin):
    @property
    def location(self) -> str:
        return "flytekit.remote.raw_script.resolver"

    @property
    def name(self) -> str:
        return "flytekit.remote.raw_script"

    def get_task_for_script(
            self,
            config: TaskLaunchConfig,
    ) -> PythonFunctionTask:
        return PythonFunctionTask(
            task_config=None,
            task_function=runner,
            task_type="raw_script",
            task_resolver=self,
            requests=config.resources,
            container_image=config.container_image,
            secret_requests=config.secrets,
            accelerator=GPUAccelerator(config.resources.accelerator) if config.resources.accelerator else None,
        )

    def load_task(self, loader_args: List[str]) -> PythonFunctionTask:
        return PythonFunctionTask(
            task_config=None,
            task_function=runner,
            task_type="workspace",
            task_resolver=self,
        )

    def loader_args(self, settings: SerializationSettings, task: PythonFunctionTask) -> List[str]:
        return ["workspace"]

    def get_all_tasks(self) -> List[Task]:
        raise NotImplementedError


resolver = RawTaskLaunchResolver()


def run(script: str, cfg: Optional[TaskLaunchConfig] = None, envvars=None,
        overwrite_cache=False, local=True, remote: Optional[FlyteRemote] = None):
    """
    Entrypoint to run scripts locally or remote.
    """
    if not cfg:
        cfg = TaskLaunchConfig()
    tk = resolver.get_task_for_script(cfg)

    ss = SerializationSettings(image_config=ImageConfig.auto_default_image())
    c = tk.get_container(ss)
    if local:
        # TODO this has to be rationalized with run.pys local execution. Many arguments have to passed
        # We should make a common local run setup method
        import os
        if envvars:
            for env_var, value in envvars.items():
                os.environ[env_var] = value
        if overwrite_cache:
            os.environ["FLYTE_LOCAL_CACHE_OVERWRITE"] = "true"
        output = tk(script_file=script, run_command=cfg.run_command or "")
        return

    # TODO Version needs to include hash of config
    remote_task = remote.register_script(tk, source_path=script, version="test") #, copy_all=cfg.copy_all)
    exe = remote.execute_remote_task_lp(remote_task, inputs={"script_file": script, "run_command": cfg.run_command},
                                  overwrite_cache=overwrite_cache)
    print(remote.generate_console_url(exe))
