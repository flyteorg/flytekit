from __future__ import absolute_import
import os as _os

if not _os.environ.get('FLYTEKIT_SETUP', None):
    from lazy_import import lazy_module as _lazy_module


class LazyLoadPlugin(object):

    LAZY_LOADING_PLUGINS = {}
    _ERROR_MSG_FMT = "Attempting to use a plugin functionality that requires module " \
                     "`{{module}}`, but it couldn't be loaded. Please `pip install flytekit[{plugin_name}]` or " \
                     "`flytekit[all]` to get these dependencies."

    def __init__(self, plugin_name, plugin_requirements, modules):
        """
        :param Text plugin_name:
        :param list[Text] plugin_requirements:
        :param list[Text] modules:
        """
        if not _os.environ.get('FLYTEKIT_SETUP', None):
            self._lazy_modules = [
                _lazy_module(module, error_strings={'msg': type(self)._ERROR_MSG_FMT.format(plugin_name=plugin_name)})
                for module in modules
            ]
        else:
            self._lazy_modules = []
        type(self).LAZY_LOADING_PLUGINS[plugin_name] = plugin_requirements

    @classmethod
    def get_extras_require(cls):
        """
        :rtype: dict[Text,list[Text]]
        """
        d = cls.LAZY_LOADING_PLUGINS.copy()
        all_plugins = []
        for k in d:
            all_plugins.extend(d[k])
        d['all'] = all_plugins
        return d
