"""
.. currentmodule:: flytekitplugins.asqav

This plugin provides cryptographic audit trails for Flyte tasks using
`Asqav <https://asqav.com>`_. Each task execution is wrapped with
cryptographically signed receipts at lifecycle points (started, finished,
failed), enabling provable, tamper-proof audit records for AI workflows in
regulated environments.

.. autosummary::
   :template: custom.rst
   :toctree: generated/

   asqav_audit
"""

from .tracking import asqav_audit

__all__ = ["asqav_audit"]
