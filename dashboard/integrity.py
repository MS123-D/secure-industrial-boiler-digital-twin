import hashlib
import json

HASH_FIELDS = ["device_id", "timestamp", "temperature", "pressure", "status"]

def canonical_payload(payload: dict) -> str:
    """
    Creates a stable string from selected fields only.
    This ensures consistent hashing across systems.
    """
    clean = {k: payload.get(k) for k in HASH_FIELDS}
    return json.dumps(clean, separators=(",", ":"), sort_keys=True)

def sha256_hash(payload: dict) -> str:
    msg = canonical_payload(payload).encode("utf-8")
    return hashlib.sha256(msg).hexdigest()

def verify_hash(payload: dict) -> bool:
    if "hash" not in payload:
        return False
    expected = sha256_hash(payload)
    return expected == payload["hash"]
