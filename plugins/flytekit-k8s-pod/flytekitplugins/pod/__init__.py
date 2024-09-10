import warnings

from .task import Pod

"""
.. currentmodule:: flytekitplugins.pod

This package contains things that are useful when extending Flytekit.

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   Pod
"""

warnings.warn(
    "This pod plugin is no longer necessary, please use the pod_template and pod_template_name options to @task as described "
    "in https://docs.flyte.org/en/latest/deployment/configuration/general.html#configuring-task-pods-with-k8s-podtemplates",
    FutureWarning,
)
