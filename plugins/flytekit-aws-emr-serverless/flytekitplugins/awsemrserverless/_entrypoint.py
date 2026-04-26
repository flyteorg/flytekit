"""
EMR Serverless Pythonic-mode entrypoint script.

This file is the canonical source of the bootstrap script that EMR
Serverless workers execute as ``sparkSubmit.entryPoint`` for Pythonic
tasks (i.e. tasks that do not provide an explicit ``spark_job_driver``).

How it is delivered to EMR
--------------------------

The connector pod:

1. reads this file from its own ``site-packages`` install;
2. computes ``hashlib.sha256(content)[:12]`` and uploads (idempotently)
   to ``s3://<bucket>/flyte/emr-serverless/entrypoint-<hash>.py``;
3. passes that S3 URI as ``StartJobRun.jobDriver.sparkSubmit.entryPoint``.

EMR Serverless then downloads this script onto the Spark driver and
runs it with ``spark-submit``.  ``sys.argv[1:]`` carries the actual
``pyflyte-fast-execute`` (or ``pyflyte-execute``) invocation that
should run inside the worker, plus the fast-registration distribution
arguments.

Why a custom entrypoint at all
------------------------------

EMR Serverless's API requires ``sparkSubmit.entryPoint`` to be a
single Python file URI -- there is no "container as entrypoint"
escape hatch (cf. SageMaker / Batch / ECS, which run the container
itself).  We need a thin shim that:

* downloads the fast-registration tarball from Flyte's blob store,
* invokes ``pyflyte-fast-execute`` with the right resolver arguments,
* converts Flytekit's "exit-0-on-user-error" semantics into a non-zero
  exit so EMR reports ``FAILED`` instead of ``SUCCESS``.

This file deliberately has only ``flytekit`` as a runtime dependency
(specifically ``flytekit.tools.fast_registration.download_distribution``)
because it runs *inside the EMR worker*, not the connector pod.

Editing this file
-----------------

Treat this file as part of the connector's *runtime contract* with
EMR workers, not as plugin internals:

* changes here propagate to every Pythonic-mode job on the next
  connector deploy via the content hash in the S3 key;
* the corresponding unit tests live in ``tests/test_entrypoint.py``
  and exercise this module both as imported Python and as the
  spawned subprocess EMR sees;
* upstream alignment: this is the EMR analogue of
  ``flytetools/flytekitplugins/databricks/entrypoint.py`` --
  same shape, different transport (S3 instead of GitHub).
"""

import os
import signal
import subprocess
import sys

from flytekit.tools.fast_registration import download_distribution


def _run_subprocess(cmd, env=None):
    """Run ``cmd`` and forward SIGTERM, returning ``(returncode, stderr_text)``.

    stdout streams through to the parent (Spark driver stdout); stderr is
    captured so the caller can inspect it for Flytekit's user-error banner.
    """
    p = subprocess.Popen(cmd, env=env, stderr=subprocess.PIPE, stdout=None)
    signal.signal(signal.SIGTERM, lambda s, f: p.send_signal(s))
    _, stderr_bytes = p.communicate()
    stderr_text = stderr_bytes.decode("utf-8", errors="replace") if stderr_bytes else ""
    if stderr_text:
        sys.stderr.write(stderr_text)
    return p.returncode, stderr_text


def _exit_with_code(rc, stderr_text=""):
    """Translate Flytekit subprocess exit semantics into EMR-correct exits.

    Flytekit's ``pyflyte-execute`` catches user exceptions, writes the
    error to the Flyte output blob, and exits ``0`` -- by design for
    K8s-based agents where FlytePropeller reads the output.

    In EMR Serverless the connector only polls EMR job state
    (``SUCCESS`` / ``FAILED``) and cannot read the output blobs.  If
    ``pyflyte-execute`` exits ``0`` but the user function failed, EMR
    reports ``SUCCESS`` and the connector wrongly reports ``SUCCEEDED``.

    Detect this by scanning stderr for Flytekit's error banner.  When
    found, force a non-zero exit so Spark fails the driver and EMR
    reports ``FAILED``.
    """
    if rc == 0 and "User Error Captured by Flyte" in stderr_text:
        print(
            "[flyte-entrypoint] pyflyte-execute exited 0 but stderr contains "
            "a user error -- forcing non-zero exit so EMR reports FAILED",
            file=sys.stderr,
        )
        sys.exit(1)
    if rc != 0:
        print(f"[flyte-entrypoint] Task process exited with code {rc}", file=sys.stderr)
    sys.exit(rc)


def _parse_fast_execute_args(args):
    """Split a ``pyflyte-fast-execute ...`` argv into its three pieces.

    Returns ``(additional_distribution, dest_dir, task_cmd_start)``
    where ``task_cmd_start`` is the index in ``args`` where the
    underlying ``pyflyte-execute ...`` command begins.

    Recognises the two-arg flag forms emitted by Flytekit:
      ``--additional-distribution <s3://...>``
      ``--dest-dir <path>``
    and the optional ``--`` separator before the inner command.
    """
    additional_distribution = None
    dest_dir = None
    task_cmd_start = 0

    i = 1
    while i < len(args):
        if args[i] == "--additional-distribution" and i + 1 < len(args):
            additional_distribution = args[i + 1]
            i += 2
        elif args[i] == "--dest-dir" and i + 1 < len(args):
            dest_dir = args[i + 1]
            i += 2
        elif args[i] == "--":
            task_cmd_start = i + 1
            break
        else:
            task_cmd_start = i
            break

    return additional_distribution, dest_dir, task_cmd_start


def _build_resolver_command(task_execute_cmd, additional_distribution, dest_dir):
    """Inject the fast-registration distribution args before ``--resolver``.

    ``pyflyte-execute`` resolves task callables via a resolver plugin
    (default ``flytekit.core.python_auto_container.default_task_resolver``).
    For fast-registered code, the resolver needs to know where the
    extracted source tree lives, which we inject as
    ``--dynamic-addl-distro`` / ``--dynamic-dest-dir`` immediately
    before ``--resolver``.
    """
    cmd = []
    for arg in task_execute_cmd:
        if arg == "--resolver":
            cmd.extend(
                [
                    "--dynamic-addl-distro",
                    additional_distribution or "",
                    "--dynamic-dest-dir",
                    dest_dir or "",
                ]
            )
        cmd.append(arg)
    return cmd


def main():
    args = sys.argv[1:]
    if not args:
        print("Usage: entrypoint.py pyflyte-fast-execute|pyflyte-execute ...", file=sys.stderr)
        sys.exit(1)

    if args[0] == "pyflyte-fast-execute":
        additional_distribution, dest_dir, task_cmd_start = _parse_fast_execute_args(args)
        task_execute_cmd = list(args[task_cmd_start:])

        if additional_distribution:
            if not dest_dir:
                dest_dir = os.getcwd()
            download_distribution(additional_distribution, dest_dir)

        cmd = _build_resolver_command(task_execute_cmd, additional_distribution, dest_dir)

        env = os.environ.copy()
        if dest_dir:
            resolved = os.path.realpath(os.path.expanduser(dest_dir))
            env["PYTHONPATH"] = resolved + os.pathsep + env.get("PYTHONPATH", "")
        rc, stderr_text = _run_subprocess(cmd, env)
        _exit_with_code(rc, stderr_text)

    elif args[0] == "pyflyte-execute":
        env = os.environ.copy()
        env.setdefault("PYTHONPATH", os.getcwd())
        rc, stderr_text = _run_subprocess(args, env)
        _exit_with_code(rc, stderr_text)

    else:
        print(f"Unrecognized command: {args}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
