import sys

from setuptools import find_packages, setup  # noqa

MIN_PYTHON_VERSION = (3, 7)
CURRENT_PYTHON = sys.version_info[:2]
if CURRENT_PYTHON < MIN_PYTHON_VERSION:
    print(
        f"Flytekit API is only supported for Python version is {MIN_PYTHON_VERSION}+. Detected you are on"
        f" version {CURRENT_PYTHON}, installation will not proceed!"
    )
    sys.exit(-1)

extras_require = {}

__version__ = "0.0.0+develop"

setup(
    name="flytekit",
    version=__version__,
    maintainer="Flyte Contributors",
    maintainer_email="admin@flyte.org",
    packages=find_packages(
        include=["flytekit", "flytekit_scripts", "plugins"],
        exclude=["boilerplate", "docs", "tests*"],
    ),
    include_package_data=True,
    url="https://github.com/flyteorg/flytekit",
    description="Flyte SDK for Python",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    entry_points={
        "console_scripts": [
            "pyflyte-execute=flytekit.bin.entrypoint:execute_task_cmd",
            "pyflyte-fast-execute=flytekit.bin.entrypoint:fast_execute_task_cmd",
            "pyflyte-map-execute=flytekit.bin.entrypoint:map_execute_task_cmd",
            "pyflyte=flytekit.clis.sdk_in_container.pyflyte:main",
            "flyte-cli=flytekit.clis.flyte_cli.main:_flyte_cli",
        ]
    },
    install_requires=[
        "flyteidl>=1.1.3,<1.2.0",
        "wheel>=0.30.0,<1.0.0",
        "pandas>=1.0.0,<2.0.0",
        "pyarrow>=4.0.0,<7.0.0",
        "click>=6.6,<9.0",
        "croniter>=0.3.20,<4.0.0",
        "deprecated>=1.0,<2.0",
        "docker>=5.0.3,<7.0.0",
        "python-dateutil>=2.1",
        "grpcio>=1.43.0,!=1.45.0,<2.0",
        "grpcio-status>=1.43,!=1.45.0",
        "importlib-metadata",
        "pyopenssl",
        "joblib",
        "protobuf>=3.6.1,<4",
        "python-json-logger>=2.0.0",
        "pytimeparse>=1.1.8,<2.0.0",
        "pytz",
        "pyyaml",
        "keyring>=18.0.1",
        "requests>=2.18.4,<3.0.0",
        "responses>=0.10.7",
        "sortedcontainers>=1.5.9,<3.0.0",
        "statsd>=3.0.0,<4.0.0",
        "urllib3>=1.22,<2.0.0",
        "wrapt>=1.0.0,<2.0.0",
        "retry==0.9.2",
        "dataclasses-json>=0.5.2",
        "marshmallow-jsonschema>=0.12.0",
        "natsort>=7.0.1",
        "docker-image-py>=0.1.10",
        "singledispatchmethod; python_version < '3.8.0'",
        "typing_extensions",
        "docstring-parser>=0.9.0",
        "diskcache>=5.2.1",
        "cloudpickle>=2.0.0",
        "cookiecutter>=1.7.3",
        "numpy<1.22.0; python_version < '3.8.0'",
    ],
    extras_require=extras_require,
    scripts=[
        "flytekit_scripts/flytekit_build_image.sh",
        "flytekit_scripts/flytekit_venv",
        "flytekit/bin/entrypoint.py",
    ],
    license="apache2",
    python_requires=">=3.7",
    classifiers=[
        "Intended Audience :: Science/Research",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
