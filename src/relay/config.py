from __future__ import annotations

import os
import socket
import uuid
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _env(name, default=None):
    v = os.environ.get(name)
    return default if v is None or v.strip() == "" else v.strip()


@dataclass(frozen=True)
class Settings:
    database_url: str = "postgresql://relay:relay@localhost:5432/relay"
    api_token: str = field(default="relay-demo-token", repr=False)
    mock_world_url: str = "http://localhost:9210"
    world_url_aliases: tuple = ("http://localhost:9210", "http://127.0.0.1:9210")
    http_timeout_s: float = 10.0
    retry_max_attempts: int = 6
    retry_base_s: float = 2.0
    retry_max_backoff_s: float = 30.0
    lease_seconds: int = 15
    poll_interval_s: float = 0.5
    ai_provider: str = "heuristic"
    ai_base_url: str = "https://openrouter.ai/api/v1"
    ai_api_key: str | None = field(default=None, repr=False)
    ai_model: str = "openai/gpt-4o-mini"
    ai_timeout_s: float = 30.0
    default_max_ai_tokens: int = 20000
    faults: tuple = ()
    data_dir: Path = REPO_ROOT / "data"
    worker_id: str = field(default_factory=lambda: f"{socket.gethostname()}-{uuid.uuid4().hex[:6]}")
    max_body_bytes: int = 1_000_000

    @classmethod
    def from_env(cls) -> "Settings":
        d = cls()
        aliases = _env("RELAY_WORLD_URL_ALIASES")
        faults = _env("RELAY_FAULTS")
        return cls(
            database_url=_env("RELAY_DATABASE_URL", d.database_url),
            api_token=_env("RELAY_API_TOKEN", d.api_token),
            mock_world_url=_env("RELAY_MOCK_WORLD_URL", d.mock_world_url).rstrip("/"),
            world_url_aliases=tuple(a.strip().rstrip("/") for a in aliases.split(",")) if aliases else d.world_url_aliases,
            http_timeout_s=float(_env("RELAY_HTTP_TIMEOUT_SECONDS", d.http_timeout_s)),
            retry_max_attempts=int(_env("RELAY_RETRY_MAX_ATTEMPTS", d.retry_max_attempts)),
            retry_base_s=float(_env("RELAY_RETRY_BASE_SECONDS", d.retry_base_s)),
            retry_max_backoff_s=float(_env("RELAY_RETRY_MAX_BACKOFF_SECONDS", d.retry_max_backoff_s)),
            lease_seconds=int(_env("RELAY_LEASE_SECONDS", d.lease_seconds)),
            poll_interval_s=float(_env("RELAY_POLL_INTERVAL_SECONDS", d.poll_interval_s)),
            ai_provider=_env("RELAY_AI_PROVIDER", d.ai_provider).lower(),
            ai_base_url=_env("RELAY_AI_BASE_URL", d.ai_base_url).rstrip("/"),
            ai_api_key=_env("RELAY_AI_API_KEY") or _env("OPENROUTER_API_KEY"),
            ai_model=_env("RELAY_AI_MODEL", d.ai_model),
            ai_timeout_s=float(_env("RELAY_AI_TIMEOUT_SECONDS", d.ai_timeout_s)),
            default_max_ai_tokens=int(_env("RELAY_DEFAULT_MAX_AI_TOKENS", d.default_max_ai_tokens)),
            faults=tuple(f.strip() for f in faults.split(",")) if faults else (),
            data_dir=Path(_env("RELAY_DATA_DIR", str(d.data_dir))),
            worker_id=_env("RELAY_WORKER_ID", d.worker_id),
        )
