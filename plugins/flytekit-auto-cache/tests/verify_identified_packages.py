from flytekitplugins.auto_cache import CacheExternalDependencies
from my_package.main import my_main_function as func


def main():
    cache = CacheExternalDependencies(salt="salt", root_dir="./my_package")
    _ = cache._get_function_dependencies(func, set())
    packages = cache.get_version_dict().keys()

    expected_packages = {'PIL', 'bs4', 'numpy', 'pandas', 'scipy', 'sklearn'}
    set(packages) == expected_packages, f"Expected keys {expected_packages}, but got {set(packages)}"

    expected_constants = {'SOME_CONSTANT': '111', 'DummyClass.some_attr': 'some_custom_attr', 'yaml.__version__': '6.0', 'mod.SOME_OTHER_CONSTANT': '222'}
    assert set(cache.constants.keys()) == set(expected_constants.keys()), f"Expected constants keys {set(expected_constants.keys())}, but got {set(cache.constants.keys())}"
    for key in expected_constants:
        assert cache.constants[key] == expected_constants[key], f"Expected value for {key} to be {expected_constants[key]}, but got {cache.constants[key]}"

if __name__ == "__main__":
    main()
