from __future__ import absolute_import

from setuptools import setup, find_packages  # noqa
import flytekit  # noqa
from flytekit.tools.lazy_loader import LazyLoadPlugin  # noqa

extras_require = LazyLoadPlugin.get_extras_require()

extras_require[':python_version<"3"'] = [
    "configparser>=3.0.0,<4.0.0",
    "futures>=3.2.0,<4.0.0",
    "pathlib2>=2.3.2,<3.0.0"
]

setup(
    name='flytekit',
    version=flytekit.__version__,
    maintainer='Lyft',
    maintainer_email='flyte-eng@lyft.com',
    packages=find_packages(exclude=["tests*"]),
    url='https://github.com/lyft/flytekit',
    description='Flyte SDK for Python',
    long_description=open('README.md').read(),
    entry_points={
        'console_scripts': [
            'pyflyte-execute=flytekit.bin.entrypoint:execute_task_cmd',
            'pyflyte=flytekit.clis.sdk_in_container.pyflyte:main',
            'flyte-cli=flytekit.clis.flyte_cli.main:_flyte_cli'
        ]
    },
    install_requires=[
        "flyteidl>=0.17.27,<1.0.0",
        "click>=6.6,<8.0",
        "croniter>=0.3.20,<4.0.0",
        "deprecation>=2.0,<3.0",
        "boto3>=1.4.4,<2.0",
        "python-dateutil<2.8.1,>=2.1",
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
        "urllib3>=1.22,<1.25",
        "wrapt>=1.0.0,<2.0.0",
    ],
    extras_require=extras_require,
    scripts=[
        'scripts/flytekit_install_spark.sh',
        'scripts/flytekit_spark_entrypoint.sh',
        'scripts/flytekit_build_image.sh',
        'scripts/flytekit_venv'
    ],
    license="apache2",
    python_requires=">=2.7"
)
