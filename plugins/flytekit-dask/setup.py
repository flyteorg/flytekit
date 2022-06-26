from setuptools import setup

PLUGIN_NAME = "dask"

microlib_name = f"flytekitplugins-{PLUGIN_NAME}"

plugin_requires = [
    "flytekit>=1.0.0,<2.0.0",
    "dask[distributed]>=2022.5.2",
    "dask-kubernetes>=2022.5.2",
]

__version__ = "0.0.0+develop"

setup(
    name=microlib_name,
    version=__version__,
    author="Bernhard Stadlbauer",
    author_email="bstadlbauer@gmx.net",
    description="Dask plugin for flytekit",
    namespace_packages=["flytekitplugins"],
    packages=[f"flytekitplugins.{PLUGIN_NAME}"],
    install_requires=plugin_requires,
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
