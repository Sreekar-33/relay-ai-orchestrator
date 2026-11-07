import secrets
import time


def new_id(prefix: str) -> str:
    """Time-ordered, collision-resistant id like run_0193f2a1b4c5d6e7f8a9."""
    return f"{prefix}_{int(time.time() * 1000):011x}{secrets.token_hex(5)}"
