from airflow.hooks.filesystem import FSHook


class FlyteFSHook:
    def __init__(self, conn_id: str = "fs_default"):
        self.conn_id = conn_id

    def get_path(self) -> str:
        return "/"


def _flyte_fs_hook(*args, **kwargs):
    return FlyteFSHook()


FSHook.__new__ = _flyte_fs_hook
