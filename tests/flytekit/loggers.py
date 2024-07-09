import sys
from subprocess import run


def test_error(tmp_path):
    EXAMPLE = "import flytekit"

    script_path = tmp_path / "script.py"
    script_path.write_text(EXAMPLE)
    out = run([sys.executable, script_path], text=True, capture_output=True)
    assert out.stderr == ""
