from __future__ import annotations

import asyncio
import json
import re
import time
from copy import deepcopy
from typing import Any, Callable, Mapping

import aiohttp

from individualStockReview.agent.template import render_structure
from individualStockReview.core.logging import get_logger, mask_sensitive_data, summarize_data

LOGGER = get_logger("agent.runners.sse_runner")


async def replace_chat_link(text: str, replace_str: str | None = None) -> str:
    pattern = r'<a class="chat_link_a" href>(\d+)</a>'

    def replace_func(match: re.Match[str]) -> str:
        if replace_str is not None:
            return replace_str
        return f"<{match.group(1)}>"

    return re.sub(pattern, replace_func, text)


async def show_quote_type(text: str, take_num_dict: dict[int, str]) -> str:
    pattern = r'<(\d+)>'

    def replace_func(match: re.Match[str]) -> str:
        number = match.group(1)
        return f"<{number}-{take_num_dict[int(number) - 1]}>"

    return re.sub(pattern, replace_func, text)


async def _parse_single_sse_event(event_str: str) -> dict[str, Any] | None:
    event: dict[str, Any] = {}
    try:
        for line in event_str.splitlines():
            line = line.strip()
            if not line:
                continue
            if line.startswith("id:"):
                event["id"] = line[3:].strip()
            elif line.startswith("data:"):
                event.setdefault("data", "")
                event["data"] += line[5:].strip()
            elif line.startswith("type:"):
                event["type"] = line[5:].strip()
            elif line.startswith("retry:"):
                try:
                    event["retry"] = int(line[6:].strip())
                except (TypeError, ValueError):
                    event["retry"] = 3000

        if "data" in event and event["data"]:
            try:
                event["data"] = json.loads(event["data"])
            except (json.JSONDecodeError, TypeError):
                pass
        return event or None
    except Exception as exc:
        LOGGER.exception("SSE 单事件解析失败: %s", exc)
        return None


async def parse_sse_chunks(chunks) -> Any:
    buffer = ""
    try:
        async for chunk in chunks:
            if not chunk:
                await asyncio.sleep(0.01)
                continue
            try:
                chunk_str = chunk.decode("utf-8", errors="ignore")
            except Exception:
                chunk_str = ""
            buffer += chunk_str
            while "\n\n" in buffer:
                event_str, buffer = buffer.split("\n\n", 1)
                event = await _parse_single_sse_event(event_str)
                if event:
                    yield event
    except (asyncio.CancelledError, Exception) as exc:
        if not isinstance(exc, asyncio.CancelledError):
            LOGGER.exception("SSE 传输中断/数据不完整: %s", exc)
        if buffer.strip():
            event = await _parse_single_sse_event(buffer)
            if event:
                yield event
        return

    if buffer.strip():
        event = await _parse_single_sse_event(buffer)
        if event:
            yield event


def _request_snapshot(request_params: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "method": request_params.get("method"),
        "url": request_params.get("url"),
        "headers": request_params.get("headers"),
        "json": request_params.get("json"),
        "timeout": request_params.get("timeout"),
    }


def _normalize_template_variables(value: Any) -> dict[str, Any]:
    return deepcopy(value) if isinstance(value, dict) else {}


class retryClass:
    @classmethod
    async def decorator(
        cls,
        retryFunc: Callable,
        apiParams: dict | None = None,
        isApiRequest: bool = True,
        retryNum: int = 3,
        retryInterval: float = 0.0,
        *args,
        **kwargs,
    ) -> tuple[bool, Any]:
        apiParams = deepcopy(apiParams or {})
        last_result: tuple[bool, Any] = (False, "未执行请求")
        for attempt in range(retryNum):
            if isApiRequest:
                if attempt == 0:
                    LOGGER.debug("发起请求: %s", summarize_data(_request_snapshot(apiParams)))
                else:
                    LOGGER.info("请求重试 %s/%s", attempt + 1, retryNum)
                last_result = await cls.apiRequestAsync(retryFunc, apiParams, *args, **kwargs)
            else:
                try:
                    last_result = (True, await retryFunc(*args, **kwargs))
                except Exception as exc:
                    last_result = (False, str(exc))
            if last_result[0]:
                return last_result
            if retryInterval > 0 and attempt < retryNum - 1:
                await asyncio.sleep(retryInterval)

        LOGGER.warning("请求重试耗尽: %s", summarize_data(mask_sensitive_data(_request_snapshot(apiParams))))
        return last_result

    @classmethod
    async def apiRequestAsync(cls, fun: Callable[[aiohttp.ClientResponse, Any], Any], apiParams: dict, *args, **kwargs) -> tuple[bool, Any]:
        request_params = deepcopy(apiParams)
        timeout_value = request_params.pop("timeout", None)
        timeout = aiohttp.ClientTimeout(total=timeout_value) if timeout_value else None
        if kwargs.get("timeStatc") and "startTime" not in kwargs:
            kwargs["startTime"] = time.perf_counter()
        started_at = time.perf_counter()
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.request(**request_params) as response:
                    elapsed = round(time.perf_counter() - started_at, 2)
                    LOGGER.debug(
                        "HTTP %s %s -> %s (%.2fs)",
                        request_params.get("method"),
                        request_params.get("url"),
                        response.status,
                        elapsed,
                    )
                    if response.status != 200:
                        raise RuntimeError(f"请求状态码错误: {response.status}")
                    result = await fun(response, *args, **kwargs)
                    return True, result
        except Exception as exc:
            LOGGER.exception("请求失败: %s", summarize_data(mask_sensitive_data(_request_snapshot(request_params))))
            return False, str(exc)


class GFChatAgent:
    def __init__(self, sessionParam: dict[str, Any], agentChatParam: dict[str, Any], logger_name: str = "agent.gf.chat"):
        self.sessionParam = deepcopy(sessionParam)
        self.agentChatParam = deepcopy(agentChatParam)
        self.logger = get_logger(logger_name)

    @staticmethod
    async def getSessionAsync(response: aiohttp.ClientResponse, *args, **kwargs) -> str:
        result = await response.json()
        return result["data"]["id"]

    @staticmethod
    async def jsonAnswerAsync(response: aiohttp.ClientResponse, *args, **kwargs) -> dict[str, Any] | str:
        total_result: dict[str, Any] = {}
        return_response: dict[str, Any] | None = None
        planner_response: dict[str, Any] | None = None
        time_dict: dict[str, float] = {}
        async for line in parse_sse_chunks(response.content.iter_any()):
            data = line.get("data") if isinstance(line, dict) else None
            if not data:
                continue
            if isinstance(data, dict) and data.get("error"):
                raise RuntimeError("流式响应-回答异常")
            if isinstance(data, dict) and "assistant_message" in data:
                return_response = data
                if kwargs.get("timeStatc") and "firstResponseTime" not in time_dict:
                    time_dict["firstResponseTime"] = round(time.perf_counter() - kwargs["startTime"], 2)
            if kwargs.get("getCot") and isinstance(data, dict) and "planner_message" in data:
                planner_response = data

        if kwargs.get("timeStatc"):
            time_dict["totalResponseTime"] = round(time.perf_counter() - kwargs["startTime"], 2)
            total_result.update(time_dict)

        if kwargs.get("getCot"):
            if not planner_response:
                raise RuntimeError("规划结果获取异常")
            planner_message = planner_response["planner_message"]
            cot_output = planner_message["plan_rounds"]["task_nodes"]["tools"]["output"]
            total_result["cotName"] = cot_output.split("\n")[0]

        if not return_response:
            raise RuntimeError("流式结果获取异常")

        assistant_message = return_response["assistant_message"]
        take_num_dict: dict[int, str] = {}
        if kwargs.get("getQuote"):
            take_num_list: list[tuple[str, str]] = []
            split_result: list[tuple[str, list[str]]] = []
            urls = assistant_message.get("urls", [])
            if urls:
                take_num_list = [(item["api_name"], item["highlight"]) for item in urls]
                take_num_dict = {idx: api_name for idx, (api_name, _) in enumerate(take_num_list)}
                pattern = r'<a[^>]*>(\d+)</a>'
                for ref in assistant_message.get("ref_list", []):
                    quote_idx_list = re.findall(pattern, ref["text"])
                    if quote_idx_list:
                        cleaned_text = await replace_chat_link(ref["text"], replace_str="")
                        split_result.append((cleaned_text, quote_idx_list))
            total_result.update({"takeNumList": take_num_list, "splitResult": split_result})

        return_result = "".join([ref["text"] for ref in assistant_message.get("ref_list", [])])
        if kwargs.get("replaceTrace"):
            return_result = await replace_chat_link(return_result)
        if kwargs.get("showQuoteType") and take_num_dict:
            return_result = await show_quote_type(return_result, take_num_dict)
        return_result = return_result.replace('`<a class="chat_link_a" href>`', '`<a class=chat_link_a href>`')

        if kwargs.get("notTranJson"):
            total_result.update({"result": return_result})
            return total_result if total_result else return_result

        try:
            parsed_result = json.loads(return_result)
        except Exception:
            pattern = r'```(?:json)?\s*\n([\s\S]*?)\n```'
            parsed_result = json.loads(re.findall(pattern, return_result)[0])

        if total_result:
            total_result.update(parsed_result)
            return total_result
        return parsed_result

    async def getChatResult(self, defaultResult: dict[str, Any] | None = None, *args, **kwargs) -> dict[str, Any]:
        default_result = deepcopy(defaultResult or {})
        default_result.update({"isSucess": False, "errInfo": ""})
        template_variables = _normalize_template_variables(kwargs.get("templateVariables") or kwargs.get("template_variables"))

        session_param = render_structure(deepcopy(self.sessionParam), template_variables)
        agent_chat_param = render_structure(deepcopy(self.agentChatParam), template_variables)
        agent_json = agent_chat_param.setdefault("json", {})

        if "sessionUpdateParams" in kwargs:
            session_update_params = render_structure(deepcopy(kwargs["sessionUpdateParams"]), template_variables)
            session_param.setdefault("json", {}).update(session_update_params)

        if "conversation_id" not in agent_json and "conversationId" not in agent_json:
            session_id = await retryClass.decorator(self.getSessionAsync, session_param)
            default_result["isSucess"] = session_id[0]
            if not session_id[0]:
                default_result["errInfo"] = session_id[1]
                return default_result
            agent_json["conversationId"] = session_id[1]

        if kwargs.get("agentUpdateParams"):
            agent_update_params = render_structure(deepcopy(kwargs["agentUpdateParams"]), template_variables)
            agent_json.update(agent_update_params)
        else:
            default_result["errInfo"] = "没有对应的agentUpdateParams"
            return default_result

        self.logger.debug(
            "调用 GF 智能体: %s",
            summarize_data(
                {
                    "session": session_param.get("json", {}),
                    "agent": agent_json,
                }
            ),
        )

        request_kwargs = deepcopy(kwargs)
        for key in ["templateVariables", "template_variables", "sessionUpdateParams", "agentUpdateParams"]:
            request_kwargs.pop(key, None)

        chat_result = await retryClass.decorator(self.jsonAnswerAsync, agent_chat_param, **request_kwargs)
        default_result["isSucess"] = chat_result[0]
        if not chat_result[0]:
            default_result["errInfo"] = chat_result[1]
            return default_result

        payload = chat_result[1]
        if isinstance(payload, dict) and "score" in payload and isinstance(payload["score"], str):
            try:
                payload["score"] = float(payload["score"])
            except ValueError:
                pass
        if isinstance(payload, dict):
            default_result.update(payload)
        else:
            default_result["result"] = payload
        return default_result
