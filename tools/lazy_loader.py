from __future__ import annotations

import importlib as _importlib
import sys as _sys
import types as _types
from typing import List


class LazyLoadPlugin(object):
    LAZY_LOADING_PLUGINS = {}

    def __init__(self, plugin_name, plugin_requirements, related_modules: List[_LazyLoadModule]):
        """
        :param Text plugin_name:
        :param list[Text] plugin_requirements:
        :param list[LazyLoadModule] related_modules:
        """
        type(self).LAZY_LOADING_PLUGINS[plugin_name] = plugin_requirements
        for m in related_modules:
            type(m).tag_with_plugin(plugin_name)

    @classmethod
    def get_extras_require(cls):
        """
        :rtype: dict[Text,list[Text]]
        """
        d = cls.LAZY_LOADING_PLUGINS.copy()
        all_plugins_spark2 = []
        all_plugins_spark3 = []
        for k in d:
            # Default to Spark 2.4.x in all-spark2 and Spark 3.x in all-spark3.
            if k != "spark3":
                all_plugins_spark2.extend(d[k])
            if k != "spark":
                all_plugins_spark3.extend(d[k])

        d["all-spark2.4"] = all_plugins_spark2
        d["all-spark3"] = all_plugins_spark3
        # all points to Spark 2.4
        d["all"] = all_plugins_spark2
        return d


def lazy_load_module(module: str) -> _types.ModuleType:
    """
    :param Text module:
    :rtype: _types.ModuleType
    """

    class LazyLoadModule(_LazyLoadModule):
        _module = module
        _lazy_submodules = dict()
        _plugins = []

    return LazyLoadModule(module)


class _LazyLoadModule(_types.ModuleType):
    _ERROR_MSG_FMT = (
        "Attempting to use a plugin functionality that requires module "
        "`{module}`, but it couldn't be loaded. Please pip install at least one of {plugins} or "
        "`flytekit[all]` to get these dependencies.\n"
        "\n"
        "Original message: {msg}"
    )

    @classmethod
    def _load(cls):
        module = _sys.modules.get(cls._module)
        if not module:
            try:
                module = _importlib.import_module(cls._module)
            except ImportError as e:
                raise ImportError(cls._ERROR_MSG_FMT.format(module=cls._module, plugins=cls._plugins, msg=e))
        return module

    def __getattribute__(self, item):
        if item in type(self)._lazy_submodules:
            return type(self)._lazy_submodules[item]
        m = type(self)._load()
        return getattr(m, item)

    def __setattr__(self, key, value):
        m = type(self)._load()
        return setattr(m, key, value)

    @classmethod
    def _add_sub_module(cls, submodule):
        """
        Add a submodule.
        :param Text submodule:  This should be a single submodule. Do NOT include periods
        :rtype: LazyLoadModule
        """
        m = cls._lazy_submodules.get(submodule)
        if not m:
            m = cls._lazy_submodules[submodule] = lazy_load_module("{}.{}".format(cls._module, submodule))
        return m

    @classmethod
    def add_sub_module(cls, submodule):
        """
        Add a submodule.
        :param Text submodule: If periods are included, it will be added recursively
        :rtype: LazyLoadModule
        """
        parts = submodule.split(".", 1)
        m = cls._add_sub_module(parts[0])
        if len(parts) > 1:
            m = type(m).add_sub_module(parts[1])
        return m

    @classmethod
    def tag_with_plugin(cls, p: LazyLoadPlugin):
        """
        :param LazyLoadPlugin p:
        """
        cls._plugins.append(p)
