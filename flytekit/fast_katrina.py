import os

import random
import string

from flytekit.tools.package_serializer import upload_package


def get_random_string(length):
    letters = string.ascii_lowercase
    return''.join(random.choice(letters) for _ in range(length))

cur_dir = os.getcwd()
upload_package(cur_dir, get_random_string(8), "s3://lyft-modelbuilder/cookbook/fastkatrina", dry_run=False)