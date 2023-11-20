"""
routing mechanism (hard, need discussion)
1. route to the notification class
ex: sendgrid, slack. pageduty, etc.

// maybe use the __module__ in python!

methods:
1. send_notification
2. check_if_config_is_valid
3. get notification secret from flytekit

error handling:
1. use try... except... to catch the error

"""
from flytekit.loggers import logger
import importlib

class BaseNotifier:
    def send_notification(self, message: str, notification_conf: dict[str, str]):
        raise NotImplementedError("send_notification function should be implemented by subclasses.")

    def get_notification_secret(self, notification_type: str) -> str:
        raise NotImplementedError("get_notification_secret function should be implemented by subclasses.")

def get_notifier(notification_type: str) -> BaseNotifier:
    try:
        # return None if notification_type is empty
        if not notification_type:
            return None

        logger.info("@@@ using get_notifier")
        package_name = "flytekitplugins.vscode"
        module = importlib.import_module(f".{notification_type}_notification", package=package_name)
        print("@@@ this is module", module)
        notifier_instance = getattr(module, f"{notification_type.capitalize()}Notifier")
        return notifier_instance()
    except Exception as e:
        logger.error(f"Cannot find the notification class for {notification_type}: {str(e)}")
        return None

