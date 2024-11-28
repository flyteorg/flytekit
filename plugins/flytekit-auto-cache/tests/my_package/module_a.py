import module_b
from scipy.linalg import norm
from cryptography.fernet import Fernet

def helper_function():
    print("Helper function")
    module_b.another_helper()
    result = norm([1, 2, 3])
    print(result)

def unused_function():
    print("Unused function")
    key = Fernet.generate_key()
    print(key)
