from dummy_functions.dummy_function import dummy_function
from dummy_functions.dummy_function_comments_formatting_change import dummy_function as dummy_function_comments_formatting_change
from dummy_functions.dummy_function_logic_change import dummy_function as dummy_function_logic_change
from flytekit.core.auto_cache import VersionParameters
from flytekitplugins.auto_cache import CacheFunctionBody


def test_get_version_with_same_function_and_salt():
    """
    Test that calling get_version with the same function and salt returns the same hash.
    """
    cache1 = CacheFunctionBody(salt="salt")
    cache2 = CacheFunctionBody(salt="salt")

    params = VersionParameters(func=dummy_function)

    # Both calls should return the same hash since the function and salt are the same
    version1 = cache1.get_version(params)
    version2 = cache2.get_version(params)

    assert version1 == version2, f"Expected {version1}, but got {version2}"


def test_get_version_with_different_salt():
    """
    Test that calling get_version with different salts returns different hashes for the same function.
    """
    cache1 = CacheFunctionBody(salt="salt1")
    cache2 = CacheFunctionBody(salt="salt2")

    params = VersionParameters(func=dummy_function)

    # The hashes should be different because the salts are different
    version1 = cache1.get_version(params)
    version2 = cache2.get_version(params)

    assert version1 != version2, f"Expected different hashes but got the same: {version1}"



def test_get_version_with_different_logic():
    """
    Test that functions with the same name but different logic produce different hashes.
    """
    cache = CacheFunctionBody(salt="salt")

    params1 = VersionParameters(func=dummy_function)
    params2 = VersionParameters(func=dummy_function_logic_change)

    version1 = cache.get_version(params1)
    version2 = cache.get_version(params2)

    assert version1 != version2, (
        f"Hashes should be different for functions with same name but different logic. "
        f"Got {version1} and {version2}"
    )

# Test functions with different names but same logic
def function_one(x: int, y: int) -> int:
    result = x + y
    return result

def function_two(x: int, y: int) -> int:
    result = x + y
    return result

def test_get_version_with_different_function_names():
    """
    Test that functions with different names but same logic produce different hashes.
    """
    cache = CacheFunctionBody(salt="salt")

    params1 = VersionParameters(func=function_one)
    params2 = VersionParameters(func=function_two)

    version1 = cache.get_version(params1)
    version2 = cache.get_version(params2)

    assert version1 != version2, (
        f"Hashes should be different for functions with different names. "
        f"Got {version1} and {version2}"
    )

def test_get_version_with_formatting_changes():
    """
    Test that changing formatting and comments but keeping the same function name
    results in the same hash.
    """

    cache = CacheFunctionBody(salt="salt")

    params1 = VersionParameters(func=dummy_function)
    params2 = VersionParameters(func=dummy_function_comments_formatting_change)

    version1 = cache.get_version(params1)
    version2 = cache.get_version(params2)

    assert version1 == version2, (
        f"Hashes should be the same for functions with same name but different formatting. "
        f"Got {version1} and {version2}"
    )
