"""
.. currentmodule:: flytekitplugins.pod

This package contains things that are useful when extending Flytekit.

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   Pod
"""

import warnings

warnings.warn(
    "This pod plugin is no longer necessary, please use the pod_template and pod_template_name options to @task instead.",
    DeprecationWarning,
)

from .task import Pod
