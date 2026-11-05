.. _design-execution:

#######################
Execution Time Support
#######################

.. tags:: Design, Basic

Most of the tasks that are written in Flytekit will be Python functions decorated with ``@task`` which turns the body of the function into a Flyte task, capable of being run independently, or included in any number of workflows. The interaction between Flytekit and these tasks do not end once they have been serialized and registered onto the Flyte control plane however. When compiled, the command that will be executed when the task is run is hardcoded into the task definition itself.

In the basic ``@task`` decorated function scenario, the command to be run will be something containing ``pyflyte-execute``, which is one of the CLIs discussed in that section.

That command, if you were to inspect a serialized task, might look something like ::

    flytekit_venv pyflyte-execute --task-module app.workflows.failing_workflows --task-name divider --inputs {{.input}} --output-prefix {{.outputPrefix}} --raw-output-data-prefix {{.rawOutputDataPrefix}}

The point of running this script, or rather the reason for having any Flyte-related logic at execution time, is purely to codify and streamline the interaction between Flyte the platform, and the function body comprising user code. The Flyte CLI is responsible for:

* I/O: The templated ``--inputs`` and ``--output-prefix`` arguments in the example command above will be filled in by the Flyte execution engine with S3 path (in the case of an AWS deployment). The ``pyflyte`` script will download the inputs to the right location in the container, and upload the results to the ``output-prefix`` location.
* Ensure that raw output data prefix configuration option, which is again filled in by the Flyte engine, is respected so that ``FlyteFile``, ``FlyteDirectory``, and ``FlyteSchema`` objects offload their data to the correct place.
* Capture and handle error reporting: Exceptions thrown in the course of task execution are captured and uploaded to the Flyte control plane for display on the Console.
* Set up helper utilities like the ``statsd`` handle, logging and logging levels, etc.
* Ensure configuration options about the Flyte backend, which are passed through by the Flyte engine, are properly loaded in Python memory.

Recovering completed results after a shutdown crash
==================================================

A native library can crash during interpreter shutdown after Flytekit has already
uploaded a task's results. By default, ``pyflyte-fast-execute`` reports the child
process's failure even when those results are available.

Set ``FLYTE_RECOVER_SHUTDOWN_CRASH=true`` in the task environment to let
``pyflyte-fast-execute`` restore the completed SDK exit status after ``SIGSEGV``,
``SIGABRT``, or ``SIGBUS``. Recovery requires a completion record from the direct
child, written after result metadata and deck handling have finished. Missing or
invalid records, crashes before upload, ordinary nonzero exits, ``SIGTERM``, and
``SIGKILL`` are not recovered. The launcher also refuses recovery after forwarding
``SIGTERM`` to the child.

Recovery preserves error reporting: an uploaded error document still describes a
failed task, and ``FLYTE_FAIL_ON_ERROR=true`` still produces exit status 1. The
launcher logs each recovered crash and does not skip shutdown hooks. A crash can
prevent remaining cleanup or hooks from finishing, so enable this option only
when the uploaded results are sufficient and unfinished cleanup is acceptable.
This option does not repair the native library or apply to standalone
``pyflyte-execute`` processes.
