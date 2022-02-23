.. _design-clis:

###################################
Command Line Interfaces and Clients
###################################

Flytekit currently ships with two CLIs, both of which rely on the same client implementation code.

*******
Clients
*******
The client code is located in ``flytekit/clients`` and there are two.

* Similar to the :ref:`design-models` files, but a bit more complex, the ``raw`` one is basically a wrapper around the Protobuf generated code, with some handling for authentication in place, and acts as a mechanism for autocompletion and comments.
* The ``friendly`` client uses the ``raw`` client, adds handling of things like pagination, and is structurally more aligned with the functionality and call pattern of the CLI itself.

.. autoclass:: flytekit.clients.friendly.SynchronousFlyteClient

.. autoclass:: flytekit.clients.raw.RawSynchronousFlyteClient

***********************
Command Line Interfaces
***********************

Flytecli
===========
``flyte-cli`` is the general CLI that can be used to talk to the Flyte control plane (Flyte Admin). It ships with flytekit as part of the Pypi package. Think of this as the ``kubectl`` for Flyte. In fact, we're working on ``flytectl`` which is under active development the completion of which will deprecate this CLI.

Think of this CLI as a network-aware (i.e. can talk to Admin) but not code-aware (doesn't need to have user code checked out) CLI. In the registration flow, this CLI is responsible for shipping the compiled Protobuf files off to Flyte Admin.

Pyflyte
=========
Unlike Flytecli, think of this CLI as code-aware, but not network-aware (the latter is not entirely true, but it's helpful to think of it that way).

This CLI is responsible for the serialization (compilation) step in the registration flow. It parses the user code, searches for tasks, workflows and launch plans, and compiles them to Protobuf files.
