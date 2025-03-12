"""
.. currentmodule:: flytekitplugins.inference

.. autosummary::
   :nosignatures:
   :template: custom.rst
   :toctree: generated/

   NIM
   NIMSecrets
   Model
   Ollama
"""

from .nim.serve import NIM, NIMSecrets
from .ollama.serve import Model, Ollama
from .vllm.serve import VLLM, HFSecret
