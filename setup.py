from setuptools import find_packages, setup  # noqa

extras_require = {}

__version__ = "0.0.0+develop"

setup(
    name="flytekit",
    version=__version__,
    maintainer="Flyte Contributors",
    maintainer_email="admin@flyte.org",
    packages=find_packages(
        include=["flytekit", "flytekit_scripts"],
        exclude=["boilerplate", "docs", "plugins", "tests*"],
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
        # Please maintain an alphabetical order in the following list
        "adlfs",
        "click>=6.6,<9.0",
        "cloudpickle>=2.0.0",
        "cookiecutter>=1.7.3",
        "croniter>=0.3.20,<4.0.0",
        "dataclasses-json>=0.5.2,<0.5.12",  # TODO: remove upper-bound after fixing change in contract
        "diskcache>=5.2.1",
        "docker>=4.0.0,<7.0.0",
        "docstring-parser>=0.9.0",
        "flyteidl>=1.10.0",
        "fsspec>=2023.3.0,<=2023.9.2",
        "gcsfs",
        "googleapis-common-protos>=1.57",
        "grpcio",
        "grpcio-status",
        "importlib-metadata",
        "joblib",
        "jsonpickle",
        "keyring>=18.0.1",
        "kubernetes>=12.0.1",
        "marshmallow-enum",
        # TODO: remove upper-bound after fixing change in contract
        "marshmallow-jsonschema>=0.12.0",
        "mashumaro>=3.9.1",
        "numpy",
        "pandas>=1.0.0,<2.0.0",
        # TODO: Remove upper-bound after protobuf community fixes it. https://github.com/flyteorg/flyte/issues/4359
        "protobuf<4.25.0",
        "pyarrow>=4.0.0,<11.0.0",
        "python-json-logger>=2.0.0",
        "pytimeparse>=1.1.8,<2.0.0",
        "pyyaml!=6.0.0,!=5.4.0,!=5.4.1",  # pyyaml is broken with cython 3: https://github.com/yaml/pyyaml/issues/601
        "requests>=2.18.4,<3.0.0",
        "rich",
        "rich_click",
        "s3fs>=0.6.0",
        "statsd>=3.0.0,<4.0.0",
        "typing_extensions",
        "urllib3>=1.22,<2.0.0",
        "wheel>=0.30.0,<1.0.0",
    ],
    extras_require=extras_require,
    scripts=[
        "flytekit_scripts/flytekit_build_image.sh",
        "flytekit_scripts/flytekit_venv",
        "flytekit/bin/entrypoint.py",
    ],
    license="apache2",
    python_requires=">=3.8,<3.12",
    classifiers=[
        "Intended Audience :: Science/Research",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
