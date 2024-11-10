import hashlib
import uuid


def gen_infra_name() -> str:
    random_uuid = uuid.uuid4().hex
    hash_object = hashlib.sha1(random_uuid.encode())
    hash = hash_object.hexdigest()[:20]
    return f"flyte-k8sdsinfra-{hash}"


def union_maps(*maps: dict) -> dict:
    composite = {}
    for m in maps:
        if m:  # Check if the map (dictionary) is not None or empty
            composite.update(m)  # Update the composite map with the contents of the current map
    return composite
