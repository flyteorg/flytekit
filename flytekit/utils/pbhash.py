# This is a module that provides hashing utilities for Protobuf objects.
import base64
import hashlib
import json

from google.protobuf import json_format
from google.protobuf.message import Message


def compute_hash(pb: Message) -> bytes:
    """
    Computes a deterministic hash in bytes for the Protobuf object.
    """
    try:
        pb_dict = json_format.MessageToDict(pb)
        # json.dumps with sorted keys to ensure stability
        stable_json_str = json.dumps(
            pb_dict, sort_keys=True, separators=(",", ":")
        )  # separators to ensure no extra spaces
    except Exception as e:
        raise ValueError(f"Failed to marshal Protobuf object {pb} to JSON with error: {e}")

    try:
        # Deterministically hash the JSON object to a byte array. Using SHA-256 for hashing here,
        # assuming it provides a consistent hash output.
        hash_obj = hashlib.sha256(stable_json_str.encode("utf-8"))
    except Exception as e:
        raise ValueError(f"Failed to hash JSON for Protobuf object {pb} with error: {e}")

    # The digest is guaranteed to be 32 bytes long
    return hash_obj.digest()


def compute_hash_string(pb: Message) -> str:
    """
    Computes a deterministic hash in base64 encoded string for the Protobuf object
    """
    hash_bytes = compute_hash(pb)
    return base64.b64encode(hash_bytes).decode("utf-8")
