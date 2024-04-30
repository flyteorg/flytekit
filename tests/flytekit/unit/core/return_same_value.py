import os
import sys


def write_output(output_dir, output_file, v):
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)  # This will create the directory if it doesn't exist
    with open(f"{output_dir}/{output_file}", "w") as f:
        f.write(str(v))


def main(*args, output_dir):
    # Generate output files for each input argument
    for i, arg in enumerate(args, start=1):
        # Using i to generate filenames like 'a', 'b', 'c', ...
        output_file = chr(ord("a") + i - 1)
        write_output(output_dir, output_file, arg)


if __name__ == "__main__":
    *inputs, output_dir = sys.argv[1:]  # Unpack all inputs except for the last one for output_dir
    main(*inputs, output_dir=output_dir)
