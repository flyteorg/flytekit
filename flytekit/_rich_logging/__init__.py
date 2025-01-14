import asyncio
import logging
import os

import click
from rich.console import Console
from rich.console import Group
from rich.live import Live
from rich.logging import RichHandler
from rich.panel import Panel
from rich.progress import Progress, BarColumn, TextColumn

import flytekit


class AsyncRichLoggerWithProgress:

    def __init__(self):
        # Log output stream and console
        self.console = Console()
        try:
            width = os.get_terminal_size().columns
        except Exception as e:
            self.console.log(f"Failed to get terminal size: {e}")
            width = 80

        self.handler = RichHandler(
            tracebacks_suppress=[click, flytekit],
            rich_tracebacks=True,
            omit_repeated_times=False,
            show_path=False,
            log_time_format="%H:%M:%S.%f",
            console=Console(width=width),
        )

        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(message)s",
            handlers=[
                RichHandler(rich_tracebacks=True, markup=True, console=self.console)
            ],
        )
        self.logger = logging.getLogger("rich")

        # Progress bar setup
        self.progress = Progress(
            TextColumn("[bold cyan]{task.description}"),
            BarColumn(bar_width=None),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        )
        self.progress_tasks = {}

    def add_progress_bar(self, task_name, total=100, **kwargs):
        """Add a progress bar."""
        task_id = self.progress.add_task(task_name, total=total, **kwargs)
        self.progress_tasks[task_name] = task_id

    async def advance_progress(self, task_name, step=1):
        """Advance a progress bar."""
        if task_name in self.progress_tasks:
            self.progress.advance(self.progress_tasks[task_name], step)

    async def log(self, message):
        """Log a message."""
        self.logger.info(message)

    async def render_live(self):
        """Render the live display with progress and logs."""
        with Live(refresh_per_second=4, console=self.console) as live:
            while True:
                # Progress Panel
                progress_panel = Panel(self.progress, title="Progress Panel", border_style="bold green")

                # Group panels
                group = Group(progress_panel)
                live.update(group)

                await asyncio.sleep(0.2)

    async def __aenter__(self):
        """Enter the context manager."""
        # Set the current instance in the context variable
        self.render_task = asyncio.create_task(self.render_live())
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager."""
        # Mark all tasks as complete and clear tasks
        self.progress_tasks.clear()
        self.render_task.cancel()


import asyncio


class SyncWrapper:
    def __init__(self, async_context_manager):
        self.async_context_manager = async_context_manager
        self._entered_context = None

    def __enter__(self):
        # Run the async __aenter__ in a new event loop
        self._entered_context = asyncio.run(self.async_context_manager.__aenter__())
        return self._entered_context

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Run the async __aexit__ in the event loop
        asyncio.run(self.async_context_manager.__aexit__(exc_type, exc_val, exc_tb))


logger = AsyncRichLoggerWithProgress()


# Helper function to get the current progress manager from context
def get_current_rich_handler() -> AsyncRichLoggerWithProgress:
    return logger


def get_current_sync_rich_handler() -> SyncWrapper:
    return SyncWrapper(get_current_rich_handler())
