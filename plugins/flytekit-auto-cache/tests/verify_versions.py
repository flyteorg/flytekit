from flytekitplugins.auto_cache import CacheExternalDependencies

# These versions should match requirements-test.txt
EXPECTED_VERSIONS = {
    "numpy": "1.24.3",
    "pandas": "2.0.3",
    "requests": "2.31.0",
    "matplotlib": "3.7.2",
    "pillow": "10.0.0",
    "scipy": "1.11.2",
    "pytest": "7.4.0",
    "urllib3": "2.0.4",
    "cryptography": "41.0.3",
    "setuptools": "68.0.0",
    "flask": "2.3.2",
    "django": "4.2.4",
    "scikit-learn": "1.3.0",
    "beautifulsoup4": "4.12.2",
    "pyyaml": "6.0",
    "fastapi": "0.100.0",
    "sqlalchemy": "2.0.36",
    "tqdm": "4.65.0",
    "pytest-mock": "3.11.0",
    "jinja2": "3.1.2",
}


def main():
    cache = CacheExternalDependencies(salt="salt", root_dir="./my_package")
    # Hydrate _external_dependencies that would be discovered by running _get_function_dependencies.
    cache._external_dependencies = set(EXPECTED_VERSIONS.keys())
    versions = cache.get_version_dict()

    # Verify that the versions extracted by the cache match the versions that are actually in the environment
    for package, expected_version in EXPECTED_VERSIONS.items():
        actual_version = versions.get(package)
        assert actual_version == expected_version, \
            f"Version mismatch for {package}. Expected {expected_version}, got {actual_version}"
    print("Package versions dict matches expected versions!")

if __name__ == "__main__":
    main()
