from typing import OrderedDict
from mock import mock
import pytest
from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.core.cache import Cache, CachePolicy, VersionParameters
from flytekit.core.task import task
from flytekit.tools.translator import get_serializable_task


class SaltCachePolicy(CachePolicy):
    def get_version(self, salt: str, params: VersionParameters) -> str:
        return salt


class ExceptionCachePolicy(CachePolicy):
    def get_version(self, salt: str, params: VersionParameters) -> str:
        raise Exception("This is an exception")


@pytest.fixture
def default_serialization_settings():
    default_image = Image(name="default", fqn="full/name", tag="some-tag")
    default_image_config = ImageConfig(default_image=default_image)
    return SerializationSettings(
        project="p", domain="d", version="v", image_config=default_image_config
    )

def test_task_arguments(default_serialization_settings):
    @task(cache=Cache(policies=SaltCachePolicy()))
    def t1(a: int) -> int:
        return a

    serialized_t1 = get_serializable_task(OrderedDict(), default_serialization_settings, t1)
    assert serialized_t1.template.metadata.discoverable == True
    assert serialized_t1.template.metadata.discovery_version == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

    @task(cache=Cache(version="a-version"))
    def t2(a: int) -> int:
        return a

    serialized_t2 = get_serializable_task(OrderedDict(), default_serialization_settings, t2)
    assert serialized_t2.template.metadata.discoverable == True
    assert serialized_t2.template.metadata.discovery_version == "a-version"

    @task(cache=Cache(version="a-version", serialize=True))
    def t3(a: int) -> int:
        return a

    serialized_t3 = get_serializable_task(OrderedDict(), default_serialization_settings, t3)
    assert serialized_t3.template.metadata.discoverable == True
    assert serialized_t3.template.metadata.discovery_version == "a-version"
    assert serialized_t3.template.metadata.cache_serializable == True

    @task(cache=Cache(version="a-version", ignored_inputs=("a",)))
    def t4(a: int) -> int:
        return a

    serialized_t4 = get_serializable_task(OrderedDict(), default_serialization_settings, t4)
    assert serialized_t4.template.metadata.discoverable == True
    assert serialized_t4.template.metadata.discovery_version == "a-version"
    assert serialized_t4.template.metadata.cache_ignore_input_vars == ("a",)

    @task(cache=Cache(version="version-overrides-policies", policies=SaltCachePolicy()))
    def t5(a: int) -> int:
        return a

    serialized_t5 = get_serializable_task(OrderedDict(), default_serialization_settings, t5)
    assert serialized_t5.template.metadata.discoverable == True
    assert serialized_t5.template.metadata.discovery_version == "version-overrides-policies"


def test_task_arguments_deprecated(default_serialization_settings):
    with pytest.raises(ValueError, match="cache_serialize, cache_version, and cache_ignore_input_vars are deprecated. Please use Cache object"):
        @task(cache=Cache(version="a-version"), cache_version="a-conflicting-version")
        def t1_fails(a: int) -> int:
            return a

    # A more realistic example where someone might set the version in the Cache object but sets the cache_serialize
    # using the deprecated cache_serialize argument
    with pytest.raises(ValueError, match="cache_serialize, cache_version, and cache_ignore_input_vars are deprecated. Please use Cache object"):
        @task(cache=Cache(version="a-version"), cache_serialize=True)
        def t2_fails(a: int) -> int:
            return a

    with pytest.raises(ValueError, match="cache_serialize, cache_version, and cache_ignore_input_vars are deprecated. Please use Cache object"):
        @task(cache=Cache(version="a-version"), cache_ignore_input_vars=("a",))
        def t3_fails(a: int) -> int:
            return a

    with pytest.raises(ValueError, match="cache_serialize, cache_version, and cache_ignore_input_vars are deprecated. Please use Cache object"):
        @task(cache=Cache(version="a-version"), cache_serialize=True, cache_version="b-version")
        def t5_fails(a: int) -> int:
            return a

    with pytest.raises(ValueError, match="cache_serialize, cache_version, and cache_ignore_input_vars are deprecated. Please use Cache object"):
        @task(cache=Cache(version="a-version"), cache_serialize=True, cache_ignore_input_vars=("a",))
        def t6_fails(a: int) -> int:
            return a

    with pytest.raises(ValueError, match="cache_serialize, cache_version, and cache_ignore_input_vars are deprecated. Please use Cache object"):
        @task(cache=Cache(version="a-version"), cache_serialize=True, cache_version="b-version", cache_ignore_input_vars=("a",))
        def t7_fails(a: int) -> int:
            return a


def test_basic_salt_cache_policy(default_serialization_settings):
    @task
    def t_notcached(a: int) -> int:
        return a

    serialized_t_notcached = get_serializable_task(OrderedDict(), default_serialization_settings, t_notcached)
    assert serialized_t_notcached.template.metadata.discoverable == False

    @task(cache=Cache(version="a-version"))
    def t_cached_explicit_version(a: int) -> int:
        return a

    serialized_t_cached_explicit_version = get_serializable_task(OrderedDict(), default_serialization_settings, t_cached_explicit_version)
    assert serialized_t_cached_explicit_version.template.metadata.discoverable == True
    assert serialized_t_cached_explicit_version.template.metadata.discovery_version == "a-version"

    @task(cache=Cache(salt="a-sprinkle-of-salt", policies=SaltCachePolicy()))
    def t_cached(a: int) -> int:
        return a + 1

    serialized_t_cached = get_serializable_task(OrderedDict(), default_serialization_settings, t_cached)
    assert serialized_t_cached.template.metadata.discoverable == True
    assert serialized_t_cached.template.metadata.discovery_version == "348b4b8c52d8868e0c202ce4d26d59906c13716197b611a0a7a215074159df79"


@mock.patch("flytekit.configuration.plugin.FlytekitPlugin.get_default_cache_policies")
def test_set_default_policies(mock_get_default_cache_policies, default_serialization_settings):
    # Enable SaltCachePolicy as the default cache policy
    mock_get_default_cache_policies.return_value = [SaltCachePolicy()]

    @task(cache=True)
    def t1(a: int) -> int:
        return a

    serialized_t1 = get_serializable_task(OrderedDict(), default_serialization_settings, t1)
    assert serialized_t1.template.metadata.discoverable == True
    assert serialized_t1.template.metadata.discovery_version == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

    @task(cache=Cache())
    def t2(a: int) -> int:
        return a

    serialized_t2 = get_serializable_task(OrderedDict(), default_serialization_settings, t2)
    assert serialized_t2.template.metadata.discoverable == True
    assert serialized_t2.template.metadata.discovery_version == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

    # Confirm that the default versions match
    assert serialized_t1.template.metadata.discovery_version == serialized_t2.template.metadata.discovery_version

    # Reset the default policies
    mock_get_default_cache_policies.return_value = []

    with pytest.raises(ValueError, match="If version is not defined then at least one cache policy needs to be set"):
        @task(cache=True)
        def t3_fails(a: int) -> int:
            return a

    with pytest.raises(ValueError, match="If version is not defined then at least one cache policy needs to be set"):
        @task(cache=Cache())
        def t4_fails(a: int) -> int:
            return a + 1

    @task(cache=Cache(version="a-version"))
    def t_cached_explicit_version(a: int) -> int:
        return a

    serialized_t_cached_explicit_version = get_serializable_task(OrderedDict(), default_serialization_settings, t_cached_explicit_version)
    assert serialized_t_cached_explicit_version.template.metadata.discoverable == True
    assert serialized_t_cached_explicit_version.template.metadata.discovery_version == "a-version"


def test_cache_policy_exception(default_serialization_settings):
    # Set the address of the ExceptionCachePolicy in the error message so that the test is robust to changes in the
    # address of the ExceptionCachePolicy class
    with pytest.raises(ValueError, match=f"Failed to generate version for cache policy <{ExceptionCachePolicy().__module__}\.{ExceptionCachePolicy().__class__.__name__} object at 0x[0-9a-fA-F]+>.*\. Please consider setting the version in the Cache definition"):
        @task(cache=Cache(policies=ExceptionCachePolicy()))
        def t_cached(a: int) -> int:
            return a
