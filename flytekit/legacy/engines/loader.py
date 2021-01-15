import importlib as _importlib

from flytekit.common.exceptions import scopes as _exception_scopes
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.configuration import sdk as _sdk_config

_ENGINE_NAME_TO_MODULES_CACHE = {
    "flyte": ("flytekit.engines.flyte.engine", "FlyteEngineFactory", None),
    "unit": ("flytekit.engines.unit.engine", "UnitTestEngineFactory", None),
    # 'local': ('flytekit.engines.local.engine', 'EngineObjectFactory', None)
}


def get_engine(engine_name=None):
    """
    :param Text engine_name:
    :rtype: flytekit.engines.common.BaseExecutionEngineFactory
    """
    engine_name = engine_name or _sdk_config.EXECUTION_ENGINE.get()

    # TODO: Allow users to plug-in their own engine code via a config
    if engine_name not in _ENGINE_NAME_TO_MODULES_CACHE:
        raise _user_exceptions.FlyteValueException(
            engine_name,
            "Could not load an engine with the identifier '{}'.  Known engines are: {}".format(
                engine_name, list(_ENGINE_NAME_TO_MODULES_CACHE.keys())
            ),
        )

    module_path, attr, engine_impl = _ENGINE_NAME_TO_MODULES_CACHE[engine_name]
    if engine_impl is None:
        module = _exception_scopes.user_entry_point(_importlib.import_module)(module_path)

        if not hasattr(module, attr):
            raise _user_exceptions.FlyteValueException(
                module,
                "Failed to load the engine because the attribute named '{}' could not be found"
                "in the module '{}'.".format(attr, module_path),
            )

        engine_impl = getattr(module, attr)()
        _ENGINE_NAME_TO_MODULES_CACHE[engine_name] = (module_path, attr, engine_impl)

    return engine_impl
