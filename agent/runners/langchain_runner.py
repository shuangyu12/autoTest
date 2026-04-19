from __future__ import annotations

import asyncio
import time
from typing import Any

from individualStockReview.core.logging import get_logger, summarize_data

LOGGER = get_logger("agent.runners.langchain_runner")


class LangChainRunner:
    def __init__(self, logger_name: str = "agent.runners.langchain_runner"):
        self.logger = get_logger(logger_name)

    @staticmethod
    def _extract_content(response: Any) -> str:
        if response is None:
            return ""
        if isinstance(response, str):
            return response
        content = getattr(response, "content", None)
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            return "\n".join(str(item) for item in content)
        return str(response)

    async def invoke_messages(self, model: Any, messages: list[Any], timeout: float | int | None = None) -> str:
        started_at = time.perf_counter()
        try:
            if hasattr(model, "ainvoke"):
                coro = model.ainvoke(messages)
            else:
                coro = asyncio.to_thread(model.invoke, messages)
            response = await asyncio.wait_for(coro, timeout=timeout) if timeout else await coro
            elapsed = round(time.perf_counter() - started_at, 2)
            self.logger.debug(
                "LangChain 调用完成: %s",
                summarize_data({"elapsed": elapsed, "message_count": len(messages)}),
            )
            return self._extract_content(response)
        except Exception as exc:
            LOGGER.exception("LangChain 调用失败: %s", exc)
            raise
