import sys
from pathlib import Path

# Add the parent directory of `my_package` to sys.path
sys.path.append(str(Path(__file__).resolve().parent))

from module_a import helper_function
from my_dir.module_in_dir import helper_in_directory
from module_c import DummyClass
import pandas as pd  # External library

def my_main_function():
    print("Main function")
    helper_in_directory()
    helper_function()
    df = pd.DataFrame({"a": [1, 2, 3]})
    print(df)
    dc = DummyClass()
    print(dc)
    dc.dummy_method()
