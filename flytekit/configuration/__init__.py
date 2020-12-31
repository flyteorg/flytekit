import logging as _logging
import os as _os

import six as _six

try:
    import pathlib as _pathlib
except ImportError:
    import pathlib2 as _pathlib  # python 2 backport


def set_flyte_config_file(config_file_path):
    """
    :param Text config_file_path:
    """
    import flytekit.configuration.common as _common
    import flytekit.configuration.internal as _internal

    if config_file_path is not None:
        original_config_file_path = config_file_path
        config_file_path = _os.path.abspath(config_file_path)
        if not _pathlib.Path(config_file_path).is_file():
            _logging.warning(
                f"No config file provided or invalid flyte config_file_path {original_config_file_path} specified."
            )
        _os.environ[_internal.CONFIGURATION_PATH.env_var] = config_file_path
    elif _internal.CONFIGURATION_PATH.env_var in _os.environ:
        _logging.debug("Deleting configuration path {} from env".format(_internal.CONFIGURATION_PATH.env_var))
        del _os.environ[_internal.CONFIGURATION_PATH.env_var]
    _common.CONFIGURATION_SINGLETON.reset_config(config_file_path)


class TemporaryConfiguration(object):
    def __init__(self, new_config_path, internal_overrides=None):
        """
        :param Text new_config_path:
        """
        import flytekit.configuration.common as _common

        self._internal_overrides = {
            _common.format_section_key("internal", k): v for k, v in _six.iteritems(internal_overrides or {})
        }
        self._new_config_path = new_config_path
        self._old_config_path = None
        self._old_internals = None

    def __enter__(self):
        import flytekit.configuration.internal as _internal

        self._old_internals = {k: _os.environ.get(k) for k in _six.iterkeys(self._internal_overrides)}
        self._old_config_path = _os.environ.get(_internal.CONFIGURATION_PATH.env_var)
        _os.environ.update(self._internal_overrides)
        set_flyte_config_file(self._new_config_path)

    def __exit__(self, exc_type, exc_val, exc_tb):
        for k, v in _six.iteritems(self._old_internals):
            if v is not None:
                _os.environ[k] = v
            else:
                _os.environ.pop(k, None)
        self._old_internals = None
        set_flyte_config_file(self._old_config_path)
