from setuptools import find_packages, setup  # noqa

# from flytekit.tools.lazy_loader import LazyLoadPlugin  # noqa
# extras_require = LazyLoadPlugin.get_extras_require()
extras_require = {
    "spark": ["pyspark>=2.4.0,<3.0.0"],
    "spark3": ["pyspark>=3.0.0"],
    "sidecar": ["k8s-proto>=0.0.3,<1.0.0"],
    "schema": ["numpy>=1.14.0,<2.0.0", "pandas>=0.22.0,<2.0.0", "pyarrow>=0.11.0,<1.0.0"],
    "hive_sensor": ["hmsclient>=0.0.1,<1.0.0"],
    "all-spark2.4": [
        "pyspark>=2.4.0,<3.0.0",
        "k8s-proto>=0.0.3,<1.0.0",
        "numpy>=1.14.0,<2.0.0",
        "pandas>=0.22.0,<2.0.0",
        "pyarrow>=0.11.0,<1.0.0",
        "hmsclient>=0.0.1,<1.0.0",
    ],
    "all-spark3": [
        "pyspark>=3.0.0",
        "k8s-proto>=0.0.3,<1.0.0",
        "numpy>=1.14.0,<2.0.0",
        "pandas>=0.22.0,<2.0.0",
        "pyarrow>=0.11.0,<1.0.0",
        "hmsclient>=0.0.1,<1.0.0",
    ],
    "all": [
        "pyspark>=2.4.0,<3.0.0",
        "k8s-proto>=0.0.3,<1.0.0",
        "numpy>=1.14.0,<2.0.0",
        "pandas>=0.22.0,<2.0.0",
        "pyarrow>=0.11.0,<1.0.0",
        "hmsclient>=0.0.1,<1.0.0",
    ],
}


setup(
    name="flytekit",
    version="0.16.0a0",
    maintainer="Lyft",
    maintainer_email="flyte-eng@lyft.com",
    packages=find_packages(exclude=["tests*"]),
    url="https://github.com/lyft/flytekit",
    description="Flyte SDK for Python",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    entry_points={
        "console_scripts": [
            "pyflyte-execute=flytekit.bin.entrypoint:execute_task_cmd",
            "pyflyte=flytekit.clis.sdk_in_container.pyflyte:main",
            "flyte-cli=flytekit.clis.flyte_cli.main:_flyte_cli",
        ]
    },
    install_requires=[
        "flyteidl>=0.18.9,<1.0.0",
        "click>=6.6,<8.0",
        "croniter>=0.3.20,<4.0.0",
        "deprecated>=1.0,<2.0",
        "boto3>=1.4.4,<2.0",
        "python-dateutil<=2.8.1,>=2.1",
        "grpcio>=1.3.0,<2.0",
        "protobuf>=3.6.1,<4",
        "pytimeparse>=1.1.8,<2.0.0",
        "pytz>=2017.2,<2018.5",
        "keyring>=18.0.1",
        "requests>=2.18.4,<3.0.0",
        "responses>=0.10.7",
        "six>=1.9.0,<2.0.0",
        "sortedcontainers>=1.5.9<3.0.0",
        "statsd>=3.0.0,<4.0.0",
        "urllib3>=1.22,<1.26.0",
        "wrapt>=1.0.0,<2.0.0",
        "papermill>=1.2.0",
        "ipykernel>=5.0.0",
        "black==19.10b0",
        "retry==0.9.2",
    ],
    extras_require=extras_require,
    scripts=[
        "scripts/flytekit_install_spark.sh",
        "scripts/flytekit_install_spark3.sh",
        "scripts/flytekit_build_image.sh",
        "scripts/flytekit_venv",
        "scripts/flytekit_sagemaker_runner.py",
    ],
    license="apache2",
    python_requires=">=3.6",
    classifiers=[
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Software Development :: Libraries",
    ],
)
