import os
from typing import Callable, Optional, Union
import memray
import time
from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.utils import ClassDecorator
from flytekit import Deck


class mem_profiling(ClassDecorator):

    def __init__(
        self,
        task_function: Optional[Callable] = None,
        **init_kwargs: dict,
    ):
        """Memray Profiling Plugin.
        Args:
        """
        self.init_kwargs = init_kwargs

        # All kwargs need to be passed up so that the function wrapping works for both
        # `@wandb_init` and `@wandb_init(...)`
        super().__init__(
            task_function,
            **init_kwargs,
        )

    def execute(self, *args, **kwargs):
        ctx = FlyteContextManager.current_context()
        is_local_execution = ctx.execution_state.is_local_execution()

        dir_name = "memray"

        if not os.path.exists(dir_name):
            os.makedirs(dir_name)

        bin_filepath = f"{dir_name}/{self.task_function.__name__}.{time.strftime('%Y%m%d%H%M%S')}.bin"
        with memray.Tracker(bin_filepath):
            output = self.task_function(*args, **kwargs)

        os.system(f"memray flamegraph {bin_filepath}")
        with open(bin_filepath, "r", encoding="ISO-8859-1") as file:
            html_content = file.read()

        Deck("flamegraph", html_content)

        # os.system(f"memray flamegraph {bin_filepath}")
        # with open(bin_filepath, "r", encoding="ISO-8859-1") as file:
        #     html_content = file.read()

        # Deck("flamegraph", html_content)

        return output

    def get_extra_config(self):
        return {}
