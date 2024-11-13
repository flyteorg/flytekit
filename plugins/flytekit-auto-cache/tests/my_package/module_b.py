# import sys
# from pathlib import Path
#
# # Add the parent directory of `my_package` to sys.path
# sys.path.append(str(Path(__file__).resolve().parent))

from module_c import third_helper

def another_helper():
    print("Another helper")
    third_helper()
