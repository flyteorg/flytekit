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
        "googleapis-common-protos>=1.57",
        "flyteidl>=1.3.5,<1.4.0",
        "wheel>=0.30.0,<1.0.0",
        "pandas>=1.0.0,<2.0.0",
        "pyarrow>=4.0.0,<11.0.0",
        "click>=6.6,<9.0",
        "croniter>=0.3.20,<4.0.0",
        "deprecated>=1.0,<2.0",
        "docker>=5.0.3,<7.0.0",
        "python-dateutil>=2.1",
        # Restrict grpcio and grpcio-status.  Version 1.50.0 pulls in a version of protobuf that is not compatible
        # with the old protobuf library (as described in https://developers.google.com/protocol-buffers/docs/news/2022-05-06)
        "grpcio>=1.50.0,<2.0",
        "grpcio-status>=1.50.0,<2.0",
        "importlib-metadata",
        "pyopenssl",
        "joblib",
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
        # TODO: We should remove mentions to the deprecated numpy
        # aliases. More details in https://github.com/flyteorg/flyte/issues/3166
        "numpy<1.24.0",
        "gitpython",
        "kubernetes>=12.0.1",
    ],
    extras_require=extras_require,
    scripts=[
        "flytekit_scripts/flytekit_build_image.sh",
        "flytekit_scripts/flytekit_venv",
        "flytekit/bin/entrypoint.py",
    ],
    license="apache2",
    python_requires=">=3.8,<3.11",
    classifiers=[
        "Intended Audience :: Science/Research",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
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
