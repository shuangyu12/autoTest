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

    @classmethod
    def _to_json_safe(cls, value: Any) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, dict):
            return {str(key): cls._to_json_safe(item) for key, item in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [cls._to_json_safe(item) for item in value]
        if hasattr(value, "model_dump"):
            return cls._to_json_safe(value.model_dump())
        if hasattr(value, "dict"):
            return cls._to_json_safe(value.dict())
        return str(value)

    async def invoke_messages_with_metadata(
        self,
        model: Any,
        messages: list[Any],
        timeout: float | int | None = None,
    ) -> dict[str, Any]:
        started_at = time.perf_counter()
        try:
            if hasattr(model, "ainvoke"):
                coro = model.ainvoke(messages)
            else:
                coro = asyncio.to_thread(model.invoke, messages)
            response = await asyncio.wait_for(coro, timeout=timeout) if timeout else await coro
            elapsed = round(time.perf_counter() - started_at, 2)
            payload = {
                "content": self._extract_content(response),
                "response_id": getattr(response, "id", "") or "",
                "response_metadata": self._to_json_safe(getattr(response, "response_metadata", None) or {}),
                "usage_metadata": self._to_json_safe(getattr(response, "usage_metadata", None) or {}),
                "additional_kwargs": self._to_json_safe(getattr(response, "additional_kwargs", None) or {}),
            }
            self.logger.debug(
                "LangChain 调用完成: %s",
                summarize_data(
                    {
                        "elapsed": elapsed,
                        "message_count": len(messages),
                        "response_id": payload["response_id"],
                        "response_metadata": payload["response_metadata"],
                        "usage_metadata": payload["usage_metadata"],
                    }
                ),
            )
            return payload
        except Exception as exc:
            LOGGER.exception("LangChain 调用失败: %s", exc)
            raise

    async def invoke_messages(self, model: Any, messages: list[Any], timeout: float | int | None = None) -> str:
        payload = await self.invoke_messages_with_metadata(model=model, messages=messages, timeout=timeout)
        return str(payload.get("content") or "")
