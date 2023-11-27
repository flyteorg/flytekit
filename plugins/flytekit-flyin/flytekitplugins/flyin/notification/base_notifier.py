from abc import ABC, abstractmethod


class BaseNotifier(ABC):
    @abstractmethod
    def send_notification(self, message: str):
        raise NotImplementedError("send_notification function should be implemented by subclasses.")

    @abstractmethod
    def get_notification_secret(self) -> str:
        raise NotImplementedError("get_notification_secret function should be implemented by subclasses.")
