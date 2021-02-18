.. _contributing:

#################
Contributor Guide
#################

First off, thank you for thinking about contributing! Below you'll find instructions that will hopefully guide you through how to fix, improve, and extend flytekit.

Please also take some time to read through the design guides, which describes the various parts of flytekit and should make contributing easier.

****************
Setup (Do Once)
****************

We recommend using a virtual environment to develop flytekit. Inside the top level flytekit repo folder ::

    virtualenv ~/.virtualenvs/flytekit
    source ~/.virtualenvs/flytekit/bin/activate
    make setup
    pip install -e .

.. note::
    It's important not to use a virtualenv that you also use for *using* flytekit. The reason is that installing a Python
    library in editable mode will link it to your source code. That is, the behavior will change as you work on the code,
    check out different branches, etc.

This will install flytekit dependencies and also install flytekit itself in editable mode. This basically links your virtual Python's ``site-packages`` with your local repo folder, allowing your local changes to take effect when the same Python interpreter runs ``import flytekit``

****************
Plugins
****************
As discussed more in the design component, flytekit plugins currently live in this flytekit repo as well, but under a different top level folder, ``plugins``. In the future, this will be separated out into a different repo. These plugins follow a `microlib <https://medium.com/@jherreras/python-microlibs-5be9461ad979>`__ structure, which will persist even if we move repos. ::

    source ~/.virtualenvs/flytekit/bin/activate
    cd plugins
    pip install -e .

This should install all the plugins in editable mode as well.

****************
Iteration
****************

Make
====
Some helpful make commands ::

    $ make
      setup        Install requirements
      fmt          Format code with black and isort
      lint         Run linters
      test         Run tests
      requirements Compile requirements

Testing
=========
Three levels of testing are available to help you

Unit Testing
--------------
Running unit tests ::

    source ~/.virtualenvs/flytekit/bin/activate
    make test

Cookbook Testing
----------------
Please see the `cookbook <https://github.com/flyteorg/flytesnacks/tree/master/cookbook>`__ and the generated `docs <https://flytecookbook.readthedocs.io/en/latest/>`__ for more information. This example repo can be cloned and run on a local Flyte cluster, or just in your IDE or other Python environment.

Follow the set up instructions for the cookbook and then override it with the version of flytekit you're interested in testing by running something like ::

    pip install https://github.com/flyteorg/flytekit/archive/a32ab82bef4d9ff53c2b7b4e69ff11f1e93858ea.zip#egg=flytekit
    # Or for a plugin
    pip install https://github.com/flyteorg/flytekit/archive/e128f66dda48bbfc6076d240d39e4221d6af2d2b.zip#subdirectory=plugins/pod&egg=flytekitplugins-pod

Change the actual link to be from your fork if you're using a fork.

End-to-end Testing
--------------------

.. TODO: Replace this with actual instructions

The Flyte developer experience team has put together an end-to-end testing framework that will spin up a K8s cluster, install Flyte onto it, and run through a series of workflows. Please contact us if you reach this stage and would like more information on this.


****************
Formatting
****************

We use [black](https://github.com/psf/black) and [isort](https://github.com/timothycrosley/isort) to autoformat code. Run the following command to execute the formatters ::

    source ~/.virtualenvs/flytekit/bin/activate
    make fmt


********************************
Releases and Project Management
********************************

Issues
========
Please submit issues to the main `Flyte repo <https://github.com/flyteorg/flyte/issues>`__.

Releasing
===========

Currently, flytekit and all its plugins share one common version. To release, contact a member of the flytekit repo maintainers or committers, and request a release. We will create a GitHub release off of master, which will automatically publish a Pypi package.

