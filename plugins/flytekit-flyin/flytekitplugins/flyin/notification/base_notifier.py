from abc import ABC, abstractmethod


class BaseNotifier(ABC):
    @abstractmethod
    def send_notification(self, message: str):
        raise NotImplementedError("send_notification function should be implemented by subclasses.")

    @abstractmethod
    def get_notification_secret(self) -> str:
        raise NotImplementedError("get_notification_secret function should be implemented by subclasses.")


class NotifierExecutor:
    def __init__(self, notifier: BaseNotifier, max_idle_seconds: int, warning_seconds_before_termination: int):
        self._notifier = notifier
        self._max_idle_seconds = max_idle_seconds
        self._max_idle_warning_seconds = max(0, max_idle_seconds - warning_seconds_before_termination)
        self._max_idle_warning_sent = False
        self._notifier.send_notification("You can connect to the VSCode server now!")

    def handle(self, idle_time: int):
        if idle_time <= self._max_idle_warning_seconds:
            self._max_idle_warning_sent = False

        if not self._max_idle_warning_sent and idle_time > self._max_idle_warning_seconds:
            self._notifier.send_notification(
                f"Reminder: The VSCode server will be terminated in {self._max_idle_seconds - idle_time} seconds."
            )
            self._max_idle_warning_sent = True

        if idle_time > self._max_idle_seconds:
            self._notifier.send_notification(
                f"VSCode server is idle for more than {self._max_idle_seconds} seconds. Terminating..."
            )
