import os

from flytekit.tools.script_mode import compress_single_script, hash_file

WORKFLOW = """
@workflow
def my_wf() -> str:
    return "hello world"
"""


def test_deterministic_hash(tmp_path):
    workflows_dir = tmp_path / "workflows"
    workflows_dir.mkdir()

    # Create dummy init file
    open(workflows_dir / "__init__.py", "a").close()
    # Write a dummy workflow
    workflow_file = workflows_dir / "hello_world.py"
    workflow_file.write_text(WORKFLOW)

    destination = tmp_path / "destination"

    compress_single_script(workflows_dir, destination, "hello_world")
    print(f"{os.listdir(tmp_path)}")

    digest, hex_digest = hash_file(destination)

    # Try again to assert digest determinism
    destination2 = tmp_path / "destination2"
    compress_single_script(workflows_dir, destination2, "hello_world")
    digest2, hex_digest2 = hash_file(destination)

    assert digest == digest2
    assert hex_digest == hex_digest2
