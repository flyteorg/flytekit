.. _design:

############################
Structure of Flytekit
############################

Flytekit is comprised of a handful of different logical components, each discusssed in greater detail in each link

* Models - These are almost Protobuf generated files.
* Authoring - This provides the core Flyte authoring experiences, allowing users to write tasks, workflows, and launch plans.
* Control Plane - The code here allows users to interact with the control plane through Python objecs.
* Execution - A small shim layer basically that handles interaction with the Flyte ecosystem at execution time.
* CLIs and Clients - Command line tools users may find themselves interacting with and the control plane client the CLIs call.

.. toctree::
   :maxdepth: 1
   :caption: Structure and Layout of Flytekit

   models
   authoring
   control_plane
   execution
   clis
