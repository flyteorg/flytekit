from flytekitplugins.auto_cache import CacheFunctionBody


# Dummy functions
def dummy_function(x: int, y: int) -> int:
    result = x + y
    return result

def dummy_function_modified(x: int, y: int) -> int:
    result = x * y
    return result


def dummy_function_with_comments_and_formatting(x: int, y: int) -> int:
    # Adding a new line here
    result = (
            x + y
    )
    # Another new line
    return result



def test_get_version_with_same_function_and_salt():
    """
    Test that calling get_version with the same function and salt returns the same hash.
    """
    cache1 = CacheFunctionBody(salt="salt")
    cache2 = CacheFunctionBody(salt="salt")

    # Both calls should return the same hash since the function and salt are the same
    version1 = cache1.get_version(dummy_function)
    version2 = cache2.get_version(dummy_function)

    assert version1 == version2, f"Expected {version1}, but got {version2}"


def test_get_version_with_different_salt():
    """
    Test that calling get_version with different salts returns different hashes for the same function.
    """
    cache1 = CacheFunctionBody(salt="salt1")
    cache2 = CacheFunctionBody(salt="salt2")

    # The hashes should be different because the salts are different
    version1 = cache1.get_version(dummy_function)
    version2 = cache2.get_version(dummy_function)

    assert version1 != version2, f"Expected different hashes but got the same: {version1}"


def test_get_version_with_different_function_source():
    """
    Test that calling get_version with different function sources returns different hashes.
    """
    cache = CacheFunctionBody(salt="salt")

    # The hash should be different because the function source has changed
    version1 = cache.get_version(dummy_function)
    version2 = cache.get_version(dummy_function_modified)

    assert version1 != version2, f"Expected different hashes but got the same: {version1} and {version2}"


def test_get_version_with_comments_and_formatting_changes():
    """
    Test that adding comments, changing formatting, or modifying the function signature
    results in a different hash.
    """
    # Modify the function by adding comments and changing the formatting
    cache = CacheFunctionBody(salt="salt")

    # Get the hash for the original dummy function
    original_version = cache.get_version(dummy_function)

    # Get the hash for the function with comments and formatting changes
    version_with_comments_and_formatting = cache.get_version(dummy_function_with_comments_and_formatting)

    # Assert that the hashes are different
    assert original_version != version_with_comments_and_formatting, f"Expected different hashes but got the same: {original_version} and {version_with_comments_and_formatting}"
