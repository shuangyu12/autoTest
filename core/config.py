from __future__ import annotations

import os
from copy import deepcopy
from pathlib import Path
from typing import Any, Mapping

import yaml

from individualStockReview import PACKAGE_ROOT

DEFAULT_API_FILE = PACKAGE_ROOT / "apiInfo.yaml"
DEFAULT_RUNTIME_FILE = PACKAGE_ROOT / "config" / "runtime.yaml"
DEFAULT_VOLCENGINE_FILE = PACKAGE_ROOT / "config" / "volcengine.yaml"
DEFAULT_VOLCENGINE_EXAMPLE_FILE = PACKAGE_ROOT / "config" / "volcengine.example.yaml"


def _expand_env_vars(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _expand_env_vars(inner_value) for key, inner_value in value.items()}
    if isinstance(value, list):
        return [_expand_env_vars(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_expand_env_vars(item) for item in value)
    if isinstance(value, str):
        return os.path.expandvars(value)
    return value


class ConfigManager:
    def __init__(
        self,
        api_file: str | Path | None = None,
        runtime_file: str | Path | None = None,
        volcengine_file: str | Path | None = None,
    ):
        self.package_root = PACKAGE_ROOT
        self.api_file = Path(api_file) if api_file else DEFAULT_API_FILE
        self.runtime_file = Path(runtime_file) if runtime_file else DEFAULT_RUNTIME_FILE
        self.volcengine_file = Path(volcengine_file) if volcengine_file else DEFAULT_VOLCENGINE_FILE
        self.api_args = self._load_yaml(self.api_file)
        self.runtime_args = self._load_yaml(self.runtime_file) if self.runtime_file.exists() else {}
        self.volcengine_args = self._load_yaml(self.volcengine_file) if self.volcengine_file.exists() else {}

    @staticmethod
    def _load_yaml(path: Path) -> dict[str, Any]:
        with path.open("r", encoding="utf-8") as file:
            return _expand_env_vars(yaml.safe_load(file) or {})

    def clone_api(self, key: str) -> dict[str, Any]:
        if key not in self.api_args:
            raise KeyError(f"未找到 API 配置: {key}")
        return deepcopy(self.api_args[key])

    def clone_provider_config(self, provider_name: str) -> dict[str, Any]:
        provider_name = str(provider_name).lower()
        provider_map = {
            "volcengine": self.volcengine_args,
            "volc": self.volcengine_args,
        }
        if provider_name not in provider_map:
            raise KeyError(f"未找到 Provider 配置: {provider_name}")
        return deepcopy(provider_map[provider_name])

    def get_runtime(self, *keys: str, default: Any = None) -> Any:
        current: Any = self.runtime_args
        for key in keys:
            if not isinstance(current, Mapping) or key not in current:
                return default
            current = current[key]
        return deepcopy(current)

    def build_session_and_agent(self, agent_key: str, session_key: str = "gfGetSession") -> tuple[dict[str, Any], dict[str, Any]]:
        session_params = self.clone_api(session_key)
        agent_params = self.clone_api(agent_key)
        agent_json = agent_params.get("json", {})
        session_json = session_params.setdefault("json", {})
        session_json.update(
            {
                "agentId": agent_json.get("userId") or agent_json.get("user_id") or session_json.get("agentId"),
                "agentBatchId": agent_json.get("botId") or agent_json.get("bot_id") or session_json.get("agentBatchId"),
            }
        )
        return session_params, agent_params
