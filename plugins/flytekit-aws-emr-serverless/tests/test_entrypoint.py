"""
Unit tests for the EMR Serverless Pythonic-mode entrypoint script.

These tests exercise the entrypoint module on two levels:

* **Pure unit tests** of the helper functions (``_parse_fast_execute_args``,
  ``_build_resolver_command``, ``_exit_with_code``) by importing them
  directly.  These are fast, deterministic, and require no subprocess.
* **Subprocess tests** that run ``python -m`` on the actual file the
  connector uploads to S3, verifying the wire-level CLI behaviour EMR
  will see.

The shape of these tests mirrors how Databricks's ``flytetools``
maintains test coverage for ``flytekitplugins/databricks/entrypoint.py``.
"""

import hashlib
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from flytekitplugins.awsemrserverless import _entrypoint


ENTRYPOINT_PATH: Path = Path(_entrypoint.__file__).resolve()


# ---------------------------------------------------------------------------
# Module-level invariants
# ---------------------------------------------------------------------------


class TestEntrypointModuleInvariants:
    """Sanity checks on the entrypoint module itself."""

    def test_entrypoint_file_exists(self):
        assert ENTRYPOINT_PATH.is_file(), (
            f"_entrypoint.py must exist at {ENTRYPOINT_PATH}; the connector uploads this file to S3 and EMR runs it."
        )

    def test_entrypoint_is_valid_python(self):
        source = ENTRYPOINT_PATH.read_text()
        compile(source, str(ENTRYPOINT_PATH), "exec")

    def test_entrypoint_has_main(self):
        assert callable(_entrypoint.main)

    def test_entrypoint_minimal_runtime_imports(self):
        """The entrypoint runs inside the EMR worker; its module-level
        imports must be available there.  ``flytekit`` is the only
        non-stdlib module we should import (specifically
        ``download_distribution``)."""
        source = ENTRYPOINT_PATH.read_text()
        assert "from flytekit.tools.fast_registration import download_distribution" in source
        assert "import boto3" not in source, (
            "_entrypoint.py must not import boto3 -- it runs in the EMR "
            "worker which may not have boto3 installed at the version "
            "the connector uses."
        )


# ---------------------------------------------------------------------------
# _parse_fast_execute_args
# ---------------------------------------------------------------------------


class TestParseFastExecuteArgs:
    """The parser splits a ``pyflyte-fast-execute ...`` argv into its parts."""

    def test_full_argv(self):
        args = [
            "pyflyte-fast-execute",
            "--additional-distribution",
            "s3://bucket/fast-abc.tar.gz",
            "--dest-dir",
            "/tmp/work",
            "pyflyte-execute",
            "--inputs",
            "s3://in",
            "--output-prefix",
            "s3://out",
        ]
        addl, dest, start = _entrypoint._parse_fast_execute_args(args)
        assert addl == "s3://bucket/fast-abc.tar.gz"
        assert dest == "/tmp/work"
        assert args[start] == "pyflyte-execute"

    def test_only_additional_distribution(self):
        args = [
            "pyflyte-fast-execute",
            "--additional-distribution",
            "s3://bucket/fast.tar.gz",
            "pyflyte-execute",
            "--inputs",
            "s3://in",
        ]
        addl, dest, start = _entrypoint._parse_fast_execute_args(args)
        assert addl == "s3://bucket/fast.tar.gz"
        assert dest is None
        assert args[start] == "pyflyte-execute"

    def test_no_distribution_flags(self):
        args = ["pyflyte-fast-execute", "pyflyte-execute", "--inputs", "s3://in"]
        addl, dest, start = _entrypoint._parse_fast_execute_args(args)
        assert addl is None
        assert dest is None
        assert args[start] == "pyflyte-execute"

    def test_explicit_double_dash_separator(self):
        args = [
            "pyflyte-fast-execute",
            "--additional-distribution",
            "s3://b/a.tar.gz",
            "--",
            "pyflyte-execute",
            "--inputs",
            "s3://in",
        ]
        addl, dest, start = _entrypoint._parse_fast_execute_args(args)
        assert addl == "s3://b/a.tar.gz"
        assert dest is None
        assert args[start] == "pyflyte-execute"

    def test_dangling_flag_does_not_crash(self):
        """``--additional-distribution`` with no value following must not crash.

        With no value, the loop falls through to the catch-all ``else`` branch
        and treats the flag itself as the start of the task command.  This is
        recoverable nonsense (the inner subprocess will fail cleanly) -- the
        important property is that the parser doesn't index past the end.
        """
        args = ["pyflyte-fast-execute", "--additional-distribution"]
        addl, dest, start = _entrypoint._parse_fast_execute_args(args)
        assert addl is None
        assert dest is None
        assert 0 <= start < len(args)


# ---------------------------------------------------------------------------
# _build_resolver_command
# ---------------------------------------------------------------------------


class TestBuildResolverCommand:
    """The resolver builder must inject ``--dynamic-addl-distro`` /
    ``--dynamic-dest-dir`` immediately before any ``--resolver`` flag."""

    def test_injects_dynamic_args_before_resolver(self):
        task_cmd = [
            "pyflyte-execute",
            "--inputs",
            "s3://in",
            "--resolver",
            "flytekit.core.python_auto_container.default_task_resolver",
        ]
        out = _entrypoint._build_resolver_command(task_cmd, "s3://b/fast.tar.gz", "/tmp/work")
        assert "--dynamic-addl-distro" in out
        assert "s3://b/fast.tar.gz" in out
        assert "--dynamic-dest-dir" in out
        assert "/tmp/work" in out
        assert out.index("--dynamic-addl-distro") < out.index("--resolver")
        assert out.index("--dynamic-dest-dir") < out.index("--resolver")

    def test_no_resolver_flag_means_no_injection(self):
        task_cmd = ["pyflyte-execute", "--inputs", "s3://in"]
        out = _entrypoint._build_resolver_command(task_cmd, "s3://x", "/tmp")
        assert out == task_cmd

    def test_handles_none_distribution_args(self):
        task_cmd = ["pyflyte-execute", "--resolver", "x"]
        out = _entrypoint._build_resolver_command(task_cmd, None, None)
        assert "--dynamic-addl-distro" in out
        assert "--dynamic-dest-dir" in out
        addl_idx = out.index("--dynamic-addl-distro")
        dest_idx = out.index("--dynamic-dest-dir")
        assert out[addl_idx + 1] == ""
        assert out[dest_idx + 1] == ""


# ---------------------------------------------------------------------------
# _exit_with_code
# ---------------------------------------------------------------------------


class TestExitWithCode:
    """The exit handler bridges Flytekit's exit semantics to EMR's
    state-reporting model.

    The contract:
      * non-zero rc → exit with rc (EMR sees FAILED)
      * rc=0 with user-error banner in stderr → exit 1 (EMR sees FAILED)
      * rc=0 without banner → exit 0 (EMR sees SUCCESS)
    """

    def test_nonzero_rc_propagates(self):
        with pytest.raises(SystemExit) as exc:
            _entrypoint._exit_with_code(42, "")
        assert exc.value.code == 42

    def test_zero_rc_clean_exits_zero(self):
        with pytest.raises(SystemExit) as exc:
            _entrypoint._exit_with_code(0, "all good")
        assert exc.value.code == 0

    def test_zero_rc_with_user_error_forces_failure(self):
        """Flytekit catches user exceptions and exits 0; we must override
        that to ensure EMR reports FAILED, not SUCCESS."""
        stderr = "Traceback ...\nUser Error Captured by Flyte: TypeError: x must be int\n"
        with pytest.raises(SystemExit) as exc:
            _entrypoint._exit_with_code(0, stderr)
        assert exc.value.code == 1


# ---------------------------------------------------------------------------
# main() dispatch
# ---------------------------------------------------------------------------


class TestMainDispatch:
    """``main`` reads ``sys.argv`` and dispatches to the right branch."""

    def test_no_args_exits_nonzero(self):
        with patch.object(sys, "argv", ["entrypoint.py"]):
            with pytest.raises(SystemExit) as exc:
                _entrypoint.main()
            assert exc.value.code == 1

    def test_unknown_command_exits_nonzero(self):
        with patch.object(sys, "argv", ["entrypoint.py", "unknown-cmd"]):
            with pytest.raises(SystemExit) as exc:
                _entrypoint.main()
            assert exc.value.code == 1

    def test_pyflyte_execute_branch_invokes_subprocess(self):
        """When given ``pyflyte-execute ...``, main runs it as a subprocess
        with PYTHONPATH defaulted to cwd and exits with rc=0."""
        argv = ["entrypoint.py", "pyflyte-execute", "--inputs", "s3://x"]
        with (
            patch.object(sys, "argv", argv),
            patch.object(_entrypoint, "_run_subprocess", return_value=(0, "")) as m_run,
        ):
            with pytest.raises(SystemExit) as exc:
                _entrypoint.main()
            assert exc.value.code == 0
            cmd_arg, env_arg = m_run.call_args[0]
            assert cmd_arg == ["pyflyte-execute", "--inputs", "s3://x"]
            assert "PYTHONPATH" in env_arg

    def test_fast_execute_branch_downloads_and_invokes(self):
        """When given ``pyflyte-fast-execute ...``, main downloads the
        distribution, builds the resolver-aware command, and exits."""
        argv = [
            "entrypoint.py",
            "pyflyte-fast-execute",
            "--additional-distribution",
            "s3://b/fast.tar.gz",
            "--dest-dir",
            "/tmp/work",
            "pyflyte-execute",
            "--resolver",
            "flytekit.core.python_auto_container.default_task_resolver",
        ]
        with (
            patch.object(sys, "argv", argv),
            patch.object(_entrypoint, "download_distribution") as m_dl,
            patch.object(_entrypoint, "_run_subprocess", return_value=(0, "")) as m_run,
        ):
            with pytest.raises(SystemExit) as exc:
                _entrypoint.main()
            assert exc.value.code == 0
            m_dl.assert_called_once_with("s3://b/fast.tar.gz", "/tmp/work")
            cmd_arg, env_arg = m_run.call_args[0]
            assert "--dynamic-addl-distro" in cmd_arg
            assert "s3://b/fast.tar.gz" in cmd_arg
            assert "/tmp/work" in env_arg["PYTHONPATH"]


# ---------------------------------------------------------------------------
# End-to-end CLI behaviour (run the file as EMR will)
# ---------------------------------------------------------------------------


class TestEntrypointAsScript:
    """Run the entrypoint file as a subprocess to verify the wire-level
    behaviour EMR's spark-submit will observe."""

    def test_no_args_exits_one_with_usage(self):
        proc = subprocess.run(
            [sys.executable, str(ENTRYPOINT_PATH)],
            capture_output=True,
            text=True,
        )
        assert proc.returncode == 1
        assert "Usage" in proc.stderr

    def test_unknown_command_exits_one(self):
        proc = subprocess.run(
            [sys.executable, str(ENTRYPOINT_PATH), "definitely-not-a-flyte-command"],
            capture_output=True,
            text=True,
        )
        assert proc.returncode == 1
        assert "Unrecognized command" in proc.stderr


# ---------------------------------------------------------------------------
# Connector ↔ entrypoint coupling
# ---------------------------------------------------------------------------


class TestConnectorEntrypointWiring:
    """Ensure the connector reads exactly this file and produces a stable,
    content-addressed hash."""

    def test_connector_path_points_at_this_module(self):
        from flytekitplugins.awsemrserverless.connector import EMRServerlessConnector

        assert EMRServerlessConnector._ENTRYPOINT_PATH == ENTRYPOINT_PATH

    def test_connector_reads_byte_identical_content(self):
        from flytekitplugins.awsemrserverless.connector import EMRServerlessConnector

        EMRServerlessConnector._entrypoint_bytes_cache = None  # bust cache
        assert EMRServerlessConnector._read_entrypoint_bytes() == ENTRYPOINT_PATH.read_bytes()

    def test_hash_is_stable_for_unchanged_content(self):
        """The S3 key contains the first 12 hex chars of sha256(content);
        as long as the file content is unchanged, the key is unchanged."""
        from flytekitplugins.awsemrserverless.connector import EMRServerlessConnector

        EMRServerlessConnector._entrypoint_bytes_cache = None
        content = EMRServerlessConnector._read_entrypoint_bytes()
        h1 = hashlib.sha256(content).hexdigest()[:12]
        h2 = hashlib.sha256(ENTRYPOINT_PATH.read_bytes()).hexdigest()[:12]
        assert h1 == h2
