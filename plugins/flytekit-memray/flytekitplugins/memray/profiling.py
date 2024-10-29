import os
import time
from typing import Callable, List, Optional

import memray
from flytekit import Deck
from flytekit.core.utils import ClassDecorator


class memray_profiling(ClassDecorator):

    def __init__(
        self,
        task_function: Optional[Callable] = None,
        memray_html_reporter: str = "flamegraph",
        memray_reporter_args: Optional[List[str]] = None,
    ):
        """Memray profiling plugin.
        Args:
            task_function (function, optional): The user function to be decorated. Defaults to None.
            memray_html_reporter (str): The name of the memray reporter which generates an html report.
                Today there is only 'flamegraph' & 'table'.
            memray_reporter_args (List[str], optional): A list of arguments to pass to the reporter commands.
                See the [flamegraph](https://bloomberg.github.io/memray/flamegraph.html#reference)
                and [table](https://bloomberg.github.io/memray/table.html#cli-reference) docs for details on supported arguments.
        """

        if memray_html_reporter not in ["flamegraph", "table"]:
            raise ValueError(
                f"{memray_html_reporter} is not a supported html reporter."
            )

        if memray_reporter_args is not None and not all(
            isinstance(arg, str) and "--" in arg for arg in memray_reporter_args
        ):
            raise ValueError(
                f"unrecognized arguments for {memray_html_reporter} reporter. Please check https://bloomberg.github.io/memray/{memray_html_reporter}.html"
            )

        self.dir_name = "memray"
        self.memray_html_reporter = memray_html_reporter
        self.memray_reporter_args = memray_reporter_args if memray_reporter_args else []

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
