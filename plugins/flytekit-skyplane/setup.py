from setuptools import setup

PLUGIN_NAME = "skyplane"

microlib_name = f"flytekitplugins-{PLUGIN_NAME}"

plugin_requires = [
    "cloudpickle",
    "flyteidl>=1.5.1",
    "flytekit>=1.6.1",
    "kubernetes",
    "skyplane"  
]

__version__ = "0.0.0+develop"

setup(
    name=microlib_name,
    version=__version__,
    author="flyteorg",  
    author_email="admin@flyte.org",  
    description="Skyplane data transfer plugin for Flytekit",
    namespace_packages=["flytekitplugins"],
    packages=[f"flytekitplugins.{PLUGIN_NAME}"],
    install_requires=plugin_requires,
    extras_require={
        # Add any optional dependencies if needed
    },
    license="apache2",
    python_requires=">=3.8",
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