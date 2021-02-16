.. _contributing::

##############
Contributing
##############

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

