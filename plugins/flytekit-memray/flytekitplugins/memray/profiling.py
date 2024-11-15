import os
import sys
import time
from typing import Callable, List, Optional

import memray
from flytekit import Deck
from flytekit.core.utils import ClassDecorator


class memray_profiling(ClassDecorator):
    def __init__(
        self,
        task_function: Optional[Callable] = None,
        native_traces: bool = False,
        trace_python_allocators: bool = False,
        follow_fork: bool = False,
        memory_interval_ms: int = 10,
        memray_html_reporter: str = "flamegraph",
        memray_reporter_args: Optional[List[str]] = None,
    ):
        """Memray profiling plugin.
        Args:
            task_function (function, optional): The user function to be decorated. Defaults to None.
            native_traces (bool): Whether or not to capture native stack frames, in addition to Python stack frames (see [Native tracking](https://bloomberg.github.io/memray/run.html#native-tracking))
            trace_python_allocators (bool): Whether or not to trace Python allocators as independent allocations. (see [Python allocators](https://bloomberg.github.io/memray/python_allocators.html#python-allocators))
            follow_fork (bool): Whether or not to continue tracking in a subprocess that is forked from the tracked process (see [Tracking across forks](https://bloomberg.github.io/memray/run.html#tracking-across-forks))
            memory_interval_ms (int): How many milliseconds to wait between sending periodic resident set size updates.
                By default, every 10 milliseconds a record is written that contains the current timestamp and the total number of bytes of virtual memory allocated by the process.
                These records are used to create the graph of memory usage over time that appears at the top of the flame graph, for instance.
                This parameter lets you adjust the frequency between updates, though you shouldn't need to change it.
            memray_html_reporter (str): The name of the memray reporter which generates an html report.
                Today there is only 'flamegraph' & 'table'.
            memray_reporter_args (List[str], optional): A list of arguments to pass to the reporter commands.
                See the [flamegraph](https://bloomberg.github.io/memray/flamegraph.html#reference)
                and [table](https://bloomberg.github.io/memray/table.html#cli-reference) docs for details on supported arguments.
        """

        if memray_html_reporter not in ["flamegraph", "table"]:
            raise ValueError(f"{memray_html_reporter} is not a supported html reporter.")

        if memray_reporter_args is not None and not all(
            isinstance(arg, str) and "--" in arg for arg in memray_reporter_args
        ):
            raise ValueError(
                f"unrecognized arguments for {memray_html_reporter} reporter. Please check https://bloomberg.github.io/memray/{memray_html_reporter}.html"
            )

        self.native_traces = native_traces
        self.trace_python_allocators = trace_python_allocators
        self.follow_fork = follow_fork
        self.memory_interval_ms = memory_interval_ms
        self.dir_name = "memray_bin"
        self.memray_html_reporter = memray_html_reporter
        self.memray_reporter_args = memray_reporter_args if memray_reporter_args else []

        super().__init__(
            task_function,
            native_traces=native_traces,
            trace_python_allocators=trace_python_allocators,
            follow_fork=follow_fork,
            memory_interval_ms=memory_interval_ms,
            memray_html_reporter=memray_html_reporter,
            memray_reporter_args=memray_reporter_args,
        )

    def execute(self, *args, **kwargs):
        if not os.path.exists(self.dir_name):
            os.makedirs(self.dir_name)

        bin_filepath = os.path.join(
            self.dir_name,
            f"{self.task_function.__name__}.{time.strftime('%Y%m%d%H%M%S')}.bin",
        )

        with memray.Tracker(
            bin_filepath,
            native_traces=self.native_traces,
            trace_python_allocators=self.trace_python_allocators,
            follow_fork=self.follow_fork,
            memory_interval_ms=self.memory_interval_ms,
        ):
            output = self.task_function(*args, **kwargs)

        self.generate_flytedeck_html(reporter=self.memray_html_reporter, bin_filepath=bin_filepath)

        return output

    def generate_flytedeck_html(self, reporter, bin_filepath):
        html_filepath = bin_filepath.replace(
            self.task_function.__name__, f"{reporter}.{self.task_function.__name__}"
        ).replace(".bin", ".html")

        memray_reporter_args_str = " ".join(self.memray_reporter_args)

        if (
            os.system(
                f"{sys.executable} -m memray {reporter} -o {html_filepath} {memray_reporter_args_str} {bin_filepath}"
            )
            == 0
        ):
            with open(html_filepath, "r", encoding="utf-8") as file:
                html_content = file.read()

            Deck(f"Memray {reporter.capitalize()}", html_content)

    def get_extra_config(self):
        return {}
