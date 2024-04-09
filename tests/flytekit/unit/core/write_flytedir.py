import sys
from pathlib import Path
import shutil

def copy_directory(input_path: Path, output_path: Path):
    if not input_path.exists() or not input_path.is_dir():
        print(f"Error: {input_path} does not exist or is not a directory.")
        return

    try:
        shutil.copytree(input_path, output_path)
        print(f"Directory {input_path} was successfully copied to {output_path}")
    except Exception as e:
        print(f"Error copying {input_path} to {output_path}: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 2:
        input_path = Path(sys.argv[1])
        output_path = Path(sys.argv[2])
        copy_directory(input_path, output_path)
    else:
        print("Usage: script.py <input_directory_path> <output_directory_path>")
