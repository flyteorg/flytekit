import module_b
from scipy.linalg import norm
from cryptography.fernet import Fernet
from utils import SOME_CONSTANT
import utils

def helper_function():
    print("Helper function")
    module_b.another_helper()
    result = norm([1, 2, 3])
    print(result)
    sum([SOME_CONSTANT, utils.THIRD_CONSTANT])

def unused_function():
    print("Unused function")
    key = Fernet.generate_key()
    print(key)
