import time
from abc import ABC, abstractmethod


class BaseNotifier(ABC):
    @abstractmethod
    def send_notification(self, message: str):
        raise NotImplementedError("send_notification function should be implemented by subclasses.")

    @abstractmethod
    def get_notification_secret(self) -> str:
        raise NotImplementedError("get_notification_secret function should be implemented by subclasses.")


class NotifierExecutor:
    def __init__(self, notifier: BaseNotifier, max_idle_seconds: int, warning_seconds_before_termination: int = 60):
        self._notifier = notifier
        self._max_idle_seconds = max_idle_seconds
        self._max_idle_warning_seconds = max(0, max_idle_seconds  - warning_seconds_before_termination)
        self._max_idle_warning_sent = False
        # self._start_time = start_time
        # self._last_reminder_sent_time = start_time
        # Notify the user that the VSCode server is ready
        self._notifier.send_notification("You can connect to the VSCode server now!")

    def handle(self, idle_time: int):
        # from flytekitplugins.flyin import (  # For circular import with vscode_lib decorator
        #     HOURS_TO_SECONDS,
        #     REMINDER_EMAIL_HOURS,
        # )

        if idle_time <= self._max_idle_warning_seconds:
            self._max_idle_warning_sent = False

        # if time.time() - self._last_reminder_sent_time > REMINDER_EMAIL_HOURS * HOURS_TO_SECONDS:
        #     hours = (time.time() - self._start_time) / HOURS_TO_SECONDS
        #     self._notifier.send_notification(f"Reminder: You have been using the VSCode server for {hours} hours now.")
        #     self._last_reminder_sent_time = time.time()

        if not self._max_idle_warning_sent and idle_time > self._max_idle_warning_seconds:
            self._notifier.send_notification(
                f"Reminder: The VSCode server will be terminated in {self._max_idle_seconds - idle_time} seconds."
            )
            self._max_idle_warning_sent = True

        if idle_time > self._max_idle_seconds:
            self._notifier.send_notification(
                f"VSCode server is idle for more than {self._max_idle_seconds} seconds. Terminating..."
            )
