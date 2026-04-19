from __future__ import annotations

from copy import deepcopy
from typing import Any

from individualStockReview.agent.langchain_eval import LangChainEvaluator
from individualStockReview.agent.runners.langchain_runner import LangChainRunner
from individualStockReview.core.config import ConfigManager
from individualStockReview.core.logging import get_logger, summarize_data


class VolcengineLangChainProvider:
    def __init__(self, config_manager: ConfigManager | None = None):
        self.config_manager = config_manager or ConfigManager()
        self.logger = get_logger("agent.providers.volcengine_langchain")

    def clone_config(self) -> dict[str, Any]:
        return self.config_manager.clone_provider_config("volcengine")

    def build_chat_model(self, override_config: dict[str, Any] | None = None):
        provider_config = self.clone_config()
        if override_config:
            provider_config.update(deepcopy(override_config))
        if not provider_config:
            raise KeyError("未找到火山配置，请提供 config/volcengine.yaml 或传入 providerConfig")
        try:
            from langchain_openai import ChatOpenAI
        except ImportError as exc:
            raise ImportError("缺少 langchain-openai 依赖，请先安装后再使用火山 LangChain 能力") from exc

        model_name = provider_config.get("model")
        if not model_name:
            raise ValueError("火山配置缺少 model 字段")
        self.logger.debug("构建 Volcengine LangChain 模型: %s", summarize_data(provider_config))
        return ChatOpenAI(
            model=model_name,
            base_url=provider_config.get("base_url"),
            api_key=provider_config.get("api_key"),
            temperature=provider_config.get("temperature", 0.0),
            timeout=provider_config.get("timeout", 600),
            max_retries=provider_config.get("max_retries", 2),
        )

    def build_evaluator(self, override_config: dict[str, Any] | None = None) -> LangChainEvaluator:
        provider_config = self.clone_config()
        if override_config:
            provider_config.update(deepcopy(override_config))
        chat_model = self.build_chat_model(override_config=provider_config)
        return LangChainEvaluator(
            chat_model=chat_model,
            provider_name=provider_config.get("provider", "volcengine"),
            model_name=provider_config.get("model", ""),
            timeout=provider_config.get("timeout", 600),
            runner=LangChainRunner(),
            logger_name="agent.langchain_eval.volcengine",
        )
