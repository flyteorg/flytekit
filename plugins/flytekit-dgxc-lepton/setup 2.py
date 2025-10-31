from setuptools import setup

PLUGIN_NAME = "dgxc-lepton"

microlib_name = f"flytekitplugins-{PLUGIN_NAME}"

plugin_requires = ["flytekit>=1.9.1,<2.0.0", "leptonai"]

__version__ = "0.0.0+develop"

setup(
    name=microlib_name,
    version=__version__,
    author="Anshul Jindal",
    author_email="ansjindal@nvidia.com",
    description="DGXC Lepton Flytekit plugin for inference endpoints",
    long_description="Flytekit DGXC Lepton Plugin - AI inference endpoints using Lepton AI infrastructure",
    long_description_content_type="text/markdown",
    packages=["flytekitplugins.dgxc_lepton"],
    install_requires=plugin_requires,
    python_requires=">=3.9",
    classifiers=[
        "Intended Audience :: Science/Research",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    entry_points={"flytekit.plugins": ["dgxc_lepton=flytekitplugins.dgxc_lepton"]},
)
