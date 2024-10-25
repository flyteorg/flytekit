import os
from typing import Callable, Optional
import memray
import time
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

        dir_name = "memray"
        memray_html_reporter = ["flamegraph", "table"]

        if not os.path.exists(dir_name):
            os.makedirs(dir_name)

        bin_filepath = f"{dir_name}/{self.task_function.__name__}.{time.strftime('%Y%m%d%H%M%S')}.bin"

        with memray.Tracker(bin_filepath):
            output = self.task_function(*args, **kwargs)

        for reporter in memray_html_reporter:
            self.generate_flytedeck_html(reporter=reporter, bin_filepath=bin_filepath)

        return output

    def generate_flytedeck_html(self, reporter, bin_filepath):
        html_reporter_constants = [
            "packed_data",
            "merge_threads",
            "memory_records",
            # "inverted",
            "temporal",
        ]
        html_filepath = bin_filepath.replace(
            self.task_function.__name__, f"{reporter}.{self.task_function.__name__}"
        ).replace(".bin", ".html")
        os.system(f"memray {reporter} -o {html_filepath} {bin_filepath}")
        with open(html_filepath, "r", encoding="utf-8") as file:
            html_content = file.read()

        for constant in html_reporter_constants:
            html_content = html_content.replace(f"{constant}", f"{reporter}_{constant}")

        # with open("output.html", "w") as f:
        #     f.write(html_content)

        Deck(f"Memray {reporter.capitalize()}", html_content)

    def get_extra_config(self):
        return {}
