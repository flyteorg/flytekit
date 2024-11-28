from flytekitplugins.auto_cache import CacheExternalDependencies
from my_package.main import my_main_function as func


def main():
    cache = CacheExternalDependencies(salt="salt", root_dir="./my_package")
    _ = cache._get_function_dependencies(func, set())
    packages = cache.get_version_dict().keys()

    expected_packages = {'PIL', 'bs4', 'numpy', 'pandas', 'scipy', 'sklearn'}
    set(packages) == expected_packages, f"Expected keys {expected_packages}, but got {set(packages)}"

if __name__ == "__main__":
    main()
