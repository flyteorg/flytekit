import os

import pip
from setuptools import setup
from setuptools.command.develop import develop
from setuptools.command.install import install

PACKAGE_NAME = "flytekitplugins-parent"
SOURCES = {
    "flytekitplugins-hive": "hive",
    "flytekitplugins-papermill": "papermill",
    "flytekitplugins-spark": "spark",
    "flytekitplugins-pod": "pod",
    "flytekitplugins-kfpytorch": "kfpytorch",
    "flytekitplugins-awssagemaker": "awssagemaker",
    "flytekitplugins-kftensorflow": "kftensorflow",
    "flytekitplugins-pandera": "pandera",
    "flytekitplugins-dolt": "flytekit-dolt",
    "flytekitplugins-sqlalchemy": "flytekitplugins-sqlalchemy",
}


def install_all_plugins(sources, develop=False):
    """
    Use pip to install all plugins
    """
    print("Installing all Flyte plugins in {} mode".format("development" if develop else "normal"))
    wd = os.getcwd()
    for k, v in sources.items():
        try:
            os.chdir(os.path.join(wd, v))
            if develop:
                pip.main(["install", "-e", "."])
            else:
                pip.main(["install", "."])
        except Exception as e:
            print("Oops, something went wrong installing", k)
            print(e)
        finally:
            os.chdir(wd)


class DevelopCmd(develop):
    """Add custom steps for the develop command"""

    def run(self):
        install_all_plugins(SOURCES, develop=True)
        develop.run(self)


class InstallCmd(install):
    """Add custom steps for the install command"""

    def run(self):
        install_all_plugins(SOURCES, develop=False)
        install.run(self)


setup(
    name=PACKAGE_NAME,
    version="0.1.0",
    author="flyteorg",
    author_email="admin@flyte.org",
    description="This is a fake package to help install all the plugins",
    license="apache2",
    classifiers=["Private :: Do Not Upload to pypi server"],
    install_requires=[],
    cmdclass={"install": InstallCmd, "develop": DevelopCmd},
)
