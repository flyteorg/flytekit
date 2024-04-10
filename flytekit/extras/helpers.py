import os
import random
import string


def rand_str(len: int) -> str:
    """Generate a random string of length `len`

    Args:
        len (int): Length of the random string

    Returns:
        str: Random string
    """
    characters = string.ascii_letters + string.digits
    return "".join(random.choices(characters, k=len))


def generate_remote_path(bucket_prefix: str, fname: str) -> str:
    """Generate a random remote path for an object in a bucket

    Args:
        bucket_prefix (str): Bucket prefix
        fname (str): Filename

    Returns:
        str: Remote object prefix
    """
    try:
        hn = os.environ["HOSTNAME"]
    except KeyError as e:
        raise e("HOSTNAME environment variable not set")
    return f"{bucket_prefix}/{rand_str(2)}/{hn}/{rand_str(32)}/{fname}"
