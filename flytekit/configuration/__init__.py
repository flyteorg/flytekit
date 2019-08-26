from __future__ import absolute_import
import os as _os
import six as _six

try:
    from pathlib import Path
except ImportError:
    from pathlib2 import Path  # python 2 backport


def set_flyte_config_file(config_file_path):
    """
    :param Text config_file_path:
    """
    import flytekit.configuration.common as _common
    import flytekit.configuration.internal as _internal
    if config_file_path is not None:
        config_file_path = _os.path.abspath(config_file_path)
    _common.CONFIGURATION_SINGLETON.reset_config(config_file_path)

    # TODO: Understand why this is the way it is
    if config_file_path is not None:
        _os.environ[_internal.CONFIGURATION_PATH.env_var] = config_file_path


class TemporaryConfiguration(object):

    def __init__(self, new_config_path, internal_overrides=None):
        """
        :param Text new_config_path:
        """
        import flytekit.configuration.common as _common
        self._internal_overrides = {
            _common.format_section_key('internal', k): v
            for k, v in _six.iteritems(internal_overrides or {})
        }
        self._new_config_path = _os.path.abspath(new_config_path) if new_config_path else None
        self._old_config_path = None
        self._old_internals = None

    def __enter__(self):
        import flytekit.configuration.internal as _internal

        self._old_internals = {
            k: _os.environ.get(k)
            for k in _six.iterkeys(self._internal_overrides)
        }
        self._old_config_path = _os.environ.get(_internal.CONFIGURATION_PATH.env_var)
        _os.environ.update(self._internal_overrides)
        self._set_flyte_config_file()

    def __exit__(self, exc_type, exc_val, exc_tb):
        set_flyte_config_file(self._old_config_path)
        for k, v in _six.iteritems(self._old_internals):
            if v is not None:
                _os.environ[k] = v
            else:
                _os.environ.pop(k, None)
        self._old_internals = None

    def _set_flyte_config_file(self):
        if self._new_config_path and Path(self._new_config_path).is_file():
            set_flyte_config_file(self._new_config_path)
        else:
            set_flyte_config_file(None)
