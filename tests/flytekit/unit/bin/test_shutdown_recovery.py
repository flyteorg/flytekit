import os
import signal
import subprocess
import sys
import tarfile
import textwrap
import time
from pathlib import Path

import pytest
from flyteidl.core.errors_pb2 import ErrorDocument
from flyteidl.core.literals_pb2 import LiteralMap

from flytekit.core.constants import ERROR_FILE_NAME, OUTPUT_FILE_NAME
from flytekit.loggers import LOGGING_FMT_ENV_VAR, LOGGING_RICH_FMT_ENV_VAR

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="Task entrypoints require POSIX signals.")


@pytest.fixture
def execution_command(tmp_path: Path) -> list[str]:
    inputs = tmp_path / "inputs.pb"
    inputs.write_bytes(LiteralMap().SerializeToString())
    task_module = tmp_path / "shutdown_task.py"
    task_module.write_text(
        textwrap.dedent(
            """\
            import atexit
            import os
            import resource
            import signal
            import time
            from pathlib import Path
            from types import FrameType

            from flytekit import current_context, task
            from flytekit.core.base_task import IgnoreOutputs

            resource.setrlimit(resource.RLIMIT_CORE, (0, 0))

            def receive_termination(signum: int, frame: FrameType | None) -> None:
                Path(os.environ["SHUTDOWN_TEST_ROOT"], "termination-received").touch()
                print("CHILD_SIGTERM_RECEIVED", flush=True)

            def shutdown(completion_path: str | None) -> None:
                root = Path(os.environ["SHUTDOWN_TEST_ROOT"])
                mode = os.environ["SHUTDOWN_TEST_MODE"]
                (root / "exit-hook-ran").touch()
                print(f"EXIT_HOOK mode={mode}", flush=True)
                if completion_path and mode in ("corrupt_record", "wrong_pid", "invalid_status"):
                    replacement = {
                        "corrupt_record": "incomplete",
                        "wrong_pid": f"{os.getppid()} 0",
                        "invalid_status": f"{os.getpid()} 2",
                    }[mode]
                    Path(completion_path).write_text(replacement)
                if mode == "forwarded_sigterm":
                    signal.signal(signal.SIGTERM, receive_termination)
                    (root / "ready").touch()
                    deadline = time.monotonic() + 20
                    while not (root / "termination-received").exists():
                        if time.monotonic() >= deadline:
                            raise RuntimeError("Parent did not forward termination")
                        time.sleep(0.01)
                if mode == "ordinary_exit":
                    os._exit(7)
                if mode in ("task_error", "record_write_failure"):
                    return
                crash_signal = {
                    "sigabrt": signal.SIGABRT,
                    "sigbus": signal.SIGBUS,
                    "sigterm": signal.SIGTERM,
                    "sigkill": signal.SIGKILL,
                }.get(mode, signal.SIGSEGV)
                os.kill(os.getpid(), crash_signal)

            @task
            def value() -> int:
                mode = os.environ["SHUTDOWN_TEST_MODE"]
                assert "_FLYTE_TASK_COMPLETION_PATH" not in os.environ
                params = current_context()
                completion_path: str | None = None
                if params.has_attr("_FLYTE_TASK_COMPLETION_PATH"):
                    completion_path = params.get("_FLYTE_TASK_COMPLETION_PATH")
                if mode == "record_write_failure" and completion_path:
                    Path(completion_path).mkdir()
                if mode != "normal":
                    atexit.register(shutdown, completion_path)
                if mode == "early_crash":
                    os.kill(os.getpid(), signal.SIGSEGV)
                if mode.startswith("task_error"):
                    raise ValueError("shutdown recovery task failure")
                if mode == "ignore_outputs":
                    raise IgnoreOutputs()
                return 42
            """
        )
    )
    startup_module = tmp_path / "sitecustomize.py"
    startup_module.write_text(
        textwrap.dedent(
            """\
            import atexit
            import os
            import resource
            import signal
            from pathlib import Path

            def crash_after_dry_run() -> None:
                root = Path(os.environ["SHUTDOWN_TEST_ROOT"])
                (root / "exit-hook-ran").touch()
                print("EXIT_HOOK mode=dry_run", flush=True)
                os.kill(os.getpid(), signal.SIGSEGV)

            if os.environ.get("SHUTDOWN_TEST_MODE") == "dry_run":
                resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
                atexit.register(crash_after_dry_run)
            """
        )
    )
    distribution = tmp_path / "code.tar.gz"
    with tarfile.open(name=distribution, mode="w:gz") as archive:
        archive.add(name=task_module, arcname=task_module.name)
        archive.add(name=startup_module, arcname=startup_module.name)
    destination = tmp_path / "code"
    destination.mkdir()
    return [
        sys.executable,
        "-m",
        "flytekit.bin.entrypoint",
        "pyflyte-fast-execute",
        "--additional-distribution",
        str(distribution),
        "--dest-dir",
        str(destination),
        "--",
        sys.executable,
        "-m",
        "flytekit.bin.entrypoint",
        "pyflyte-execute",
        "--inputs",
        str(inputs),
        "--output-prefix",
        str(tmp_path / "outputs"),
        "--raw-output-data-prefix",
        str(tmp_path / "raw"),
        "--resolver",
        "flytekit.core.python_auto_container.default_task_resolver",
        "--",
        "task-module",
        "shutdown_task",
        "task-name",
        "value",
    ]


@pytest.mark.parametrize(
    ("mode", "recovery_enabled", "expected_exit", "artifact"),
    [
        pytest.param("normal", False, 0, "output", id="normal-default"),
        pytest.param("normal", True, 0, "output", id="normal-enabled"),
        pytest.param("sigsegv", False, 245, "output", id="segv-default"),
        pytest.param("sigsegv", True, 0, "output", id="recover-segv"),
        pytest.param("sigabrt", True, 0, "output", id="recover-abrt"),
        pytest.param("sigbus", True, 0, "output", id="recover-bus"),
        pytest.param("early_crash", True, 245, None, id="early-crash"),
        pytest.param("upload_failure", True, 245, None, id="upload-failure"),
        pytest.param("sigterm", True, 241, "output", id="direct-term"),
        pytest.param("sigkill", True, 247, "output", id="direct-kill"),
        pytest.param("forwarded_sigterm", True, 245, "output", id="forwarded-term"),
        pytest.param("ordinary_exit", True, 7, "output", id="ordinary-exit"),
        pytest.param("task_error", True, 1, "error", id="task-error"),
        pytest.param("task_error_crash", True, 1, "error", id="failed-task-crash"),
        pytest.param("task_error_protocol", True, 0, "error", id="error-document-protocol"),
        pytest.param("ignore_outputs", True, 245, None, id="ignored-outputs"),
        pytest.param("dry_run", True, 245, None, id="dry-run"),
        pytest.param("corrupt_record", True, 245, "output", id="corrupt-record"),
        pytest.param("wrong_pid", True, 245, "output", id="wrong-child"),
        pytest.param("invalid_status", True, 245, "output", id="invalid-status"),
        pytest.param("record_write_failure", True, 0, "output", id="record-write-failure"),
    ],
)
def test_shutdown_recovery(
    tmp_path: Path,
    execution_command: list[str],
    mode: str,
    recovery_enabled: bool,
    expected_exit: int,
    artifact: str | None,
) -> None:
    if mode == "upload_failure":
        (tmp_path / "outputs").write_text("The upload destination is a file, not a directory.")
    if mode == "dry_run":
        execution_command.insert(execution_command.index("--resolver"), "--test")
    env: dict[str, str] = {
        **os.environ,
        "SHUTDOWN_TEST_ROOT": str(tmp_path),
        "SHUTDOWN_TEST_MODE": mode,
        "FLYTE_RECOVER_SHUTDOWN_CRASH": str(recovery_enabled),
        "FLYTE_FAIL_ON_ERROR": str(mode != "task_error_protocol"),
        LOGGING_FMT_ENV_VAR: "json",
        LOGGING_RICH_FMT_ENV_VAR: "0",
    }
    with subprocess.Popen(
        args=execution_command,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    ) as process:
        try:
            if mode == "forwarded_sigterm":
                deadline: float = time.monotonic() + 30
                while not (tmp_path / "ready").exists():
                    assert process.poll() is None, "Launcher exited before the shutdown hook was ready"
                    assert time.monotonic() < deadline, "Shutdown hook did not become ready"
                    time.sleep(0.01)
                process.send_signal(signal.SIGTERM)
            output: str = process.communicate(timeout=45)[0]
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=10)
    (tmp_path / "launcher.log").write_text(output)
    output_file = tmp_path / "outputs" / OUTPUT_FILE_NAME
    error_file = tmp_path / "outputs" / ERROR_FILE_NAME
    print(f"mode={mode} recovery={recovery_enabled} exit={process.returncode}")
    for line in output.splitlines():
        if "EXIT_HOOK" in line or "CHILD_SIGTERM_RECEIVED" in line or "restoring" in line.lower():
            print(line)
    if artifact == "output":
        result = LiteralMap.FromString(output_file.read_bytes())
        assert result.literals["o0"].scalar.primitive.integer == 42
        assert not error_file.exists()
        print("outputs.pb: o0=42; error.pb: absent")
    elif artifact == "error":
        error = ErrorDocument.FromString(error_file.read_bytes())
        assert "shutdown recovery task failure" in error.error.message
        assert not output_file.exists()
        print(f"error.pb: code={error.error.code}; outputs.pb: absent")
    else:
        assert not output_file.exists()
        assert not error_file.exists()
        print("outputs.pb: absent; error.pb: absent")
    if mode == "forwarded_sigterm":
        assert (tmp_path / "termination-received").exists()
    if mode not in ("normal", "early_crash"):
        assert (tmp_path / "exit-hook-ran").exists()
    assert process.returncode == expected_exit, output
