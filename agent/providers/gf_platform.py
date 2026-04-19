from __future__ import annotations

from individualStockReview.agent.runners.sse_runner import GFChatAgent
from individualStockReview.core.config import ConfigManager
from individualStockReview.core.logging import get_logger


class GFPlatformProvider:
    def __init__(self, config_manager: ConfigManager | None = None):
        self.config_manager = config_manager or ConfigManager()
        self.logger = get_logger("agent.providers.gf_platform")

    def build_agent(self, agent_key: str, session_key: str = "gfGetSession") -> GFChatAgent:
        session_params, agent_params = self.config_manager.build_session_and_agent(agent_key, session_key=session_key)
        self.logger.debug("构建 GF 智能体: agent_key=%s session_key=%s", agent_key, session_key)
        return GFChatAgent(session_params, agent_params, logger_name=f"agent.gf.{agent_key}")

    def clone_api(self, api_key: str) -> dict:
        return self.config_manager.clone_api(api_key)
