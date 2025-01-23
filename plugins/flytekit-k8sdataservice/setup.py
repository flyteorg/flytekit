from setuptools import find_namespace_packages, setup

PLUGIN_NAME = "k8sdataservice"

microlib_name = f"flytekitplugins-{PLUGIN_NAME}"

plugin_requires = ["flytekit>=1.11.0,<2.0.0", "kubernetes>=23.6.0,<24.0.0", "flyteidl>=1.11.0,<2.0.0"]

__version__ = "0.0.0+develop"

setup(
    name=microlib_name,
    version=__version__,
    author="LinkedIn",
    author_email="shuliang@linkedin.com",
    description="Flytekit K8s Data Service Plugin",
    namespace_packages=["flytekitplugins"],
    packages=find_namespace_packages(where="."),
    include_package_data=True,
    install_requires=plugin_requires,
    license="apache2",
    python_requires=">=3.9",
    classifiers=[
        "Intended Audience :: Science/Research",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    entry_points={"flytekit.plugins": [f"{PLUGIN_NAME}=flytekitplugins.{PLUGIN_NAME}"]},
)
