import os
from typing import Callable, Optional, List
import memray
import time
from flytekit.core.utils import ClassDecorator
from flytekit import Deck


class memray_profiling(ClassDecorator):

    def __init__(
        self,
        task_function: Optional[Callable] = None,
        memray_html_reporter: str = "flamegraph",
        memray_reporter_args: Optional[List[str]] = [],
    ):
        """Memray Profiling Plugin.
        Args:
        """
        if memray_html_reporter not in ["flamegraph", "table"]:
            raise ValueError(
                f"{memray_html_reporter} is not a supported html reporter."
            )

        self.dir_name = "memray"
        self.memray_html_reporter = memray_html_reporter
        self.memray_reporter_args = memray_reporter_args

        # All kwargs need to be passed up so that the function wrapping works for both
        # `@wandb_init` and `@wandb_init(...)`
        super().__init__(
            task_function,
            memray_html_reporter=memray_html_reporter,
            memray_reporter_args=memray_reporter_args,
        )

    def execute(self, *args, **kwargs):

        if not os.path.exists(self.dir_name):
            os.makedirs(self.dir_name)

        bin_filepath = f"{self.dir_name}/{self.task_function.__name__}.{time.strftime('%Y%m%d%H%M%S')}.bin"

        with memray.Tracker(bin_filepath):
            output = self.task_function(*args, **kwargs)

        self.generate_flytedeck_html(
            reporter=self.memray_html_reporter, bin_filepath=bin_filepath
        )

        return output

    def generate_flytedeck_html(self, reporter, bin_filepath):
        html_filepath = bin_filepath.replace(
            self.task_function.__name__, f"{reporter}.{self.task_function.__name__}"
        ).replace(".bin", ".html")

        memray_reporter_args_str = " ".join(self.memray_reporter_args)

        if (
            os.system(
                f"memray {reporter} -o {html_filepath} {memray_reporter_args_str} {bin_filepath}"
            )
            == 0
        ):
            with open(html_filepath, "r", encoding="utf-8") as file:
                html_content = file.read()

            Deck(f"Memray {reporter.capitalize()}", html_content)

    def get_extra_config(self):
        return {}
