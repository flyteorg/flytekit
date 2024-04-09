import sys
from pathlib import Path


def copy_content_to_output(input_path: Path, output_path: Path):
    content = input_path.read_text()

    output_path.write_text(content)


if __name__ == "__main__":
    if len(sys.argv) > 2:
        input_path = Path(sys.argv[1])
        output_path = Path(sys.argv[2])
        copy_content_to_output(input_path, output_path)
    else:
        print("Usage: script.py <input_path> <output_path>")
