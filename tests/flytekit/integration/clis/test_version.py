import subprocess
import re

def run_info_command() -> str:
    """
    Runs the `pyflyte info` command and returns its output.
    """
    out = subprocess.run(
        ["pyflyte", "info"],
        capture_output=True,  # Capture the output streams
        text=True,  # Return outputs as strings (not bytes)
    )
    # Ensure the command ran successfully
    assert out.returncode == 0, (f"Command failed with return code {out.returncode}.\n"
                                 f"Standard Output: {out.stdout}\n"
                                 f"Standard Error: {out.stderr}\n")
    return out.stdout

def test_info_command_versions():
    """
    Tests that the `pyflyte info` command outputs the correct version information.
    """
    output = run_info_command()

    # Check that Flytekit Version is displayed
    assert re.search(r"Flytekit Version: \S+", output), "Flytekit Version not found in output."

    # Check that Flyte Backend Version is displayed
    assert re.search(r"Flyte Backend Version: \S+", output), "Flyte Backend Version not found in output."

    # Check that Flyte Backend Endpoint is displayed
    assert re.search(r"Flyte Backend Endpoint: \S+", output), "Flyte Backend Endpoint not found in output."
