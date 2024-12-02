from flytekitplugins.auto_cache import CachePrivateModules
from my_package.main import my_main_function as func



def test_dependencies():
    expected_dependencies = {
        "module_a.helper_function",
        "module_b.another_helper",
        "module_c.DummyClass.dummy_method",
        "module_c.DummyClass.other_dummy_method",
        "module_c.third_helper",
        "module_d.fourth_helper",
        "my_dir.module_in_dir.helper_in_directory",
        "my_dir.module_in_dir.other_helper_in_directory",
    }

    cache = CachePrivateModules(salt="salt", root_dir="./my_package")
    actual_dependencies = cache._get_function_dependencies(func, set())

    actual_dependencies_str = {
        f"{dep.__module__}.{dep.__qualname__}".replace("my_package.", "")
        for dep in actual_dependencies
    }

    assert actual_dependencies_str == expected_dependencies, (
        f"Dependencies do not match:\n"
        f"Expected: {expected_dependencies}\n"
        f"Actual: {actual_dependencies_str}"
    )

test_dependencies()
