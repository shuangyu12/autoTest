from __future__ import annotations

from individualStockReview.agent.providers.gf_platform import GFPlatformProvider
from individualStockReview.agent.providers.volcengine_langchain import VolcengineLangChainProvider
from individualStockReview.core.config import ConfigManager


class AgentRegistry:
    def __init__(
        self,
        api_file: str | None = None,
        runtime_file: str | None = None,
        volcengine_file: str | None = None,
    ):
        self.config_manager = ConfigManager(api_file=api_file, runtime_file=runtime_file, volcengine_file=volcengine_file)
        self.gf_provider = GFPlatformProvider(config_manager=self.config_manager)
        self.volcengine_provider = VolcengineLangChainProvider(config_manager=self.config_manager)

    def get_provider(self, provider_name: str):
        provider_name = str(provider_name).lower()
        if provider_name == "gf":
            return self.gf_provider
        if provider_name in {"volcengine", "volc"}:
            return self.volcengine_provider
        raise KeyError(f"未注册的 provider: {provider_name}")

    def get_gf_agent(self, agent_key: str, session_key: str = "gfGetSession"):
        return self.gf_provider.build_agent(agent_key=agent_key, session_key=session_key)

    def get_volcengine_evaluator(self, override_config: dict | None = None):
        return self.volcengine_provider.build_evaluator(override_config=override_config)

    def clone_api(self, api_key: str) -> dict:
        return self.config_manager.clone_api(api_key)

    def clone_provider_config(self, provider_name: str) -> dict:
        return self.config_manager.clone_provider_config(provider_name)
