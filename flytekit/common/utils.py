import flytekit as _flytekit
from flytekit.configuration import sdk as _sdk_config
from flytekit.core.utils import _dnsify
from flytekit.models.core import identifier as _identifier


def get_version_message():
    return "Welcome to Flyte! Version: {}".format(_flytekit.__version__)


class ExitStack(object):
    def __init__(self, entered_stack=None):
        self._contexts = entered_stack

    def enter_context(self, context):
        out = context.__enter__()
        self._contexts.append(context)
        return out

    def pop_all(self):
        entered_stack = self._contexts
        self._contexts = None
        return ExitStack(entered_stack=entered_stack)

    def __enter__(self):
        if self._contexts is not None:
            raise Exception("A non-empty context stack cannot be entered.")
        self._contexts = []
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        first_exception = None
        if self._contexts is not None:
            while len(self._contexts) > 0:
                try:
                    self._contexts.pop().__exit__(exc_type, exc_val, exc_tb)
                except Exception as ex:
                    # Catch all to try to clean up all exits before re-raising the first exception
                    if first_exception is None:
                        first_exception = ex
        if first_exception is not None:
            raise first_exception
        return False


def fqdn(module, name, entity_type=None):
    """
    :param Text module:
    :param Text name:
    :param int entity_type: _identifier.ResourceType enum
    :rtype: Text
    """
    fmt = _sdk_config.NAME_FORMAT.get()
    if entity_type == _identifier.ResourceType.WORKFLOW:
        fmt = _sdk_config.WORKFLOW_NAME_FORMAT.get() or fmt
    elif entity_type == _identifier.ResourceType.TASK:
        fmt = _sdk_config.TASK_NAME_FORMAT.get() or fmt
    elif entity_type == _identifier.ResourceType.LAUNCH_PLAN:
        fmt = _sdk_config.LAUNCH_PLAN_NAME_FORMAT.get() or fmt
    return fmt.format(module=module, name=name)


def fqdn_safe(module, key, entity_type=None):
    """
    :param Text module:
    :param Text key:
    :param int entity_type: _identifier.ResourceType enum
    :rtype: Text
    """
    return _dnsify(fqdn(module, key, entity_type=entity_type))
