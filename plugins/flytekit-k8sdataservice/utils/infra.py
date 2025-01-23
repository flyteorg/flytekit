import hashlib
import uuid


def gen_infra_name() -> str:
    random_uuid = uuid.uuid4().hex
    hash_object = hashlib.sha256(random_uuid.encode())
    hash_value = hash_object.hexdigest()[:20]
    return f"flyte-k8sdsinfra-{hash_value}"
