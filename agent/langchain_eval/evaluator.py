from __future__ import annotations

import json
import re
from copy import deepcopy
from typing import Any

from individualStockReview.agent.runners.langchain_runner import LangChainRunner
from individualStockReview.agent.template import render_template
from individualStockReview.core.compat import build_langchain_eval_result, coerce_score, ensure_legacy_defaults
from individualStockReview.core.logging import get_logger, summarize_data

LOGGER = get_logger("agent.langchain_eval.evaluator")


class LangChainEvaluator:
    def __init__(
        self,
        chat_model: Any,
        provider_name: str,
        model_name: str,
        timeout: float | int | None = None,
        runner: LangChainRunner | None = None,
        logger_name: str = "agent.langchain_eval.evaluator",
    ):
        self.chat_model = chat_model
        self.provider_name = provider_name
        self.model_name = model_name
        self.timeout = timeout
        self.runner = runner or LangChainRunner()
        self.logger = get_logger(logger_name)

    @staticmethod
    def _merge_template_variables(*sources: Any) -> dict[str, Any]:
        merged: dict[str, Any] = {}
        for source in sources:
            if isinstance(source, dict):
                merged.update(deepcopy(source))
        return merged

    @staticmethod
    def _try_parse_json(text: str) -> dict[str, Any] | None:
        try:
            parsed = json.loads(text)
            return parsed if isinstance(parsed, dict) else None
        except Exception:
            pass
        pattern = r"```(?:json)?\s*\n([\s\S]*?)\n```"
        matches = re.findall(pattern, text)
        if not matches:
            return None
        try:
            parsed = json.loads(matches[0])
            return parsed if isinstance(parsed, dict) else None
        except Exception:
            return None

    async def evaluate(
        self,
        default_result: dict[str, Any] | None = None,
        prompt_template: str | None = None,
        system_prompt: str | None = None,
        template_variables: dict[str, Any] | None = None,
        parse_json: bool = True,
        keep_missing: bool = True,
    ) -> dict[str, Any]:
        payload = ensure_legacy_defaults(default_result or {}, build_langchain_eval_result())
        variables = self._merge_template_variables(payload, template_variables)
        prompt_text = prompt_template or payload.get("messages") or ""
        rendered_prompt = render_template(prompt_text, variables, keep_missing=keep_missing)
        rendered_system_prompt = render_template(system_prompt, variables, keep_missing=keep_missing) if system_prompt else None
        payload.update(
            {
                "messages": rendered_prompt,
                "provider": self.provider_name,
                "model": self.model_name,
                "isSucess": False,
                "errInfo": "",
            }
        )

        messages: list[Any] = []
        try:
            from langchain_core.messages import HumanMessage, SystemMessage

            if rendered_system_prompt:
                messages.append(SystemMessage(content=rendered_system_prompt))
            messages.append(HumanMessage(content=rendered_prompt))
        except ImportError as exc:
            payload["errInfo"] = f"缺少 langchain_core 依赖: {exc}"
            return payload

        self.logger.debug(
            "执行 LangChain 评测: %s",
            summarize_data({"provider": self.provider_name, "model": self.model_name, "messages": rendered_prompt}),
        )

        try:
            raw_output = await self.runner.invoke_messages(self.chat_model, messages, timeout=self.timeout)
            parsed_output = self._try_parse_json(raw_output) if parse_json else None
            if parsed_output:
                payload.update(parsed_output)
            else:
                payload["result"] = raw_output
            payload["score"] = coerce_score(payload.get("score", ""))
            payload["isSucess"] = True
        except Exception as exc:
            LOGGER.exception("LangChain 评测失败: %s", exc)
            payload["errInfo"] = str(exc)
        return payload
