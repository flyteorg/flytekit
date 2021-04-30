import shlex
import subprocess
import urllib.request

from setuptools import setup
from setuptools.command.develop import develop

PLUGIN_NAME = "dolt"

microlib_name = f"flytekitplugins.{PLUGIN_NAME}"

plugin_requires = ["flytekit>=0.16.0b0,<1.0.0", "dolt_integrations>=0.1.3"]

__version__ = "0.0.0+develop"


class PostDevelopCommand(develop):
    """Post-installation for development mode."""

    def run(self):
        develop.run(self)
        install, _ = urllib.request.urlretrieve(
            "https://github.com/liquidata-inc/dolt/releases/latest/download/install.sh"
        )
        subprocess.call(shlex.split(f"chmod +x {install}"))
        subprocess.call(shlex.split(f"sudo {install}"))

        pref = "dolt config --global --add"
        subprocess.call(
            shlex.split(f"{pref} user.email bojack@horseman.com"),
        )
        subprocess.call(
            shlex.split(f"{pref} user.name 'Bojack Horseman'"),
        )
        subprocess.call(
            shlex.split(f"{pref} metrics.host eventsapi.awsdev.ld-corp.com"),
        )
        subprocess.call(shlex.split(f"{pref} metrics.port 443"))


setup(
    name=microlib_name,
    version=__version__,
    author="dolthub",
    author_email="max@dolthub.com",
    description="Dolt plugin for flytekit",
    namespace_packages=["flytekitplugins"],
    packages=[f"flytekitplugins.{PLUGIN_NAME}"],
    install_requires=plugin_requires,
    cmdclass=dict(develop=PostDevelopCommand),
    license="apache2",
    python_requires=">=3.7",
    classifiers=[
        "Intended Audience :: Science/Research",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    scripts=["scripts/flytekit_install_dolt.sh"],
)
