from abc import ABC, abstractmethod

from flytekit.loggers import logger


class BaseNotifier(ABC):
    @abstractmethod
    def send_notification(self, message: str):
        raise NotImplementedError("send_notification function should be implemented by subclasses.")

    # @classmethod
    @abstractmethod
    def get_notification_secret(self) -> str:
        raise NotImplementedError("get_notification_secret function should be implemented by subclasses.")

# def get_notification_secret(notification_type: str) -> str:
#     try:
#         return flytekit.current_context().secrets.get(notification_type, "token")
#     except Exception as e:
#         logger.error(
#             f"Cannot find the {notification_type} notification secret.\n\
#                         Error: {e}"
#         )
#         return ""
