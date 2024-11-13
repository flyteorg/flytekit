# import sys
# from pathlib import Path
#
# # Add the parent directory of `my_package` to sys.path
# sys.path.append(str(Path(__file__).resolve().parent))

# from module_b import another_helper
import module_b

def helper_function():
    print("Helper function")
    module_b.another_helper()
    # another_helper()

def unused_function():
    print("Unused function")
