from __future__ import annotations

import asyncio
import json
import re
import time
from copy import deepcopy
from typing import Any, Callable, Mapping

import aiohttp
from urllib.parse import urlparse

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
    SESSION_REFRESH_HOSTS = {"irmp-cot-uat.gf.com.cn"}
    COMPLETIONS_PATH_SUFFIX = "/outside/sse/completions"
    CONVERSATIONS_PATH_SUFFIX = "/outside/sse/conversations"

    def __init__(self, sessionParam: dict[str, Any], agentChatParam: dict[str, Any], logger_name: str = "agent.gf.chat"):
        self.sessionParam = deepcopy(sessionParam)
        self.agentChatParam = deepcopy(agentChatParam)
        self.logger = get_logger(logger_name)

    @classmethod
    def _resolve_session_strategy(cls, request_params: Mapping[str, Any]) -> dict[str, Any]:
        request_json = request_params.get("json")
        if not isinstance(request_json, Mapping):
            request_json = {}

        parsed_url = urlparse(str(request_params.get("url") or "").strip())
        host = parsed_url.netloc.lower()
        path = parsed_url.path.lower()
        has_conversation_id = "conversationId" in request_json
        has_conversation_id_snake = "conversation_id" in request_json
        is_managed_completion = host in cls.SESSION_REFRESH_HOSTS and path.endswith(cls.COMPLETIONS_PATH_SUFFIX)
        is_fixed_conversation = host in cls.SESSION_REFRESH_HOSTS and path.endswith(cls.CONVERSATIONS_PATH_SUFFIX)

        if has_conversation_id_snake or is_fixed_conversation:
            return {
                "host": host,
                "path": path,
                "target_field": "conversation_id",
                "should_refresh": False,
                "reason": "conversation_id 走 conversations 接口，框架不刷新 sessionId",
            }

        if is_managed_completion and not has_conversation_id_snake:
            return {
                "host": host,
                "path": path,
                "target_field": "conversationId",
                "should_refresh": True,
                "reason": "conversationId 走 completions 接口，框架会先调用 gfGetSession 刷新 sessionId",
            }

        return {
            "host": host,
            "path": path,
            "target_field": "conversationId" if has_conversation_id else None,
            "should_refresh": False,
            "reason": "当前接口未命中 GF session 自动刷新规则，沿用请求里的原始会话参数",
        }

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
        default_result.update({"isSucess": False, "errorInfo": ""})
        template_variables = _normalize_template_variables(kwargs.get("templateVariables") or kwargs.get("template_variables"))

        session_param = render_structure(deepcopy(self.sessionParam), template_variables)
        agent_chat_param = render_structure(deepcopy(self.agentChatParam), template_variables)
        agent_json = agent_chat_param.setdefault("json", {})
        session_json = session_param.setdefault("json", {})

        if kwargs.get("sessionName"):
            session_json["sessionName"] = render_structure(str(kwargs["sessionName"]), template_variables)
        if "sessionUpdateParams" in kwargs:
            session_update_params = render_structure(deepcopy(kwargs["sessionUpdateParams"]), template_variables)
            session_json.update(session_update_params)

        refresh_session = bool(kwargs.get("refreshSession", kwargs.get("forceNewSession", True)))
        session_strategy = self._resolve_session_strategy(agent_chat_param)
        target_field = session_strategy.get("target_field") or "conversationId"
        refresh_reason = "refreshSession=True，重新获取 sessionId" if refresh_session else "refreshSession=False，沿用原始 sessionId"

        self.logger.debug(
            "GF session策略: host=%s path=%s target=%s refresh=%s reason=%s",
            session_strategy.get("host"),
            session_strategy.get("path"),
            target_field,
            refresh_session,
            refresh_reason,
        )

        if refresh_session:
            session_id = await retryClass.decorator(self.getSessionAsync, session_param)
            default_result["isSucess"] = session_id[0]
            if not session_id[0]:
                default_result["errorInfo"] = session_id[1]
                return default_result
            agent_json[target_field] = session_id[1]

        if kwargs.get("agentUpdateParams"):
            agent_update_params = render_structure(deepcopy(kwargs["agentUpdateParams"]), template_variables)
            agent_json.update(agent_update_params)
        else:
            default_result["errorInfo"] = "没有对应的agentUpdateParams"
            return default_result

        self.logger.debug(
            "调用 GF 智能体: %s",
            summarize_data(
                {
                    "session": session_json,
                    "agent": agent_json,
                }
            ),
        )

        request_kwargs = deepcopy(kwargs)
        for key in [
            "templateVariables",
            "template_variables",
            "sessionName",
            "sessionUpdateParams",
            "agentUpdateParams",
            "forceNewSession",
            "refreshSession",
        ]:
            request_kwargs.pop(key, None)

        chat_result = await retryClass.decorator(self.jsonAnswerAsync, agent_chat_param, **request_kwargs)
        default_result["isSucess"] = chat_result[0]
        if not chat_result[0]:
            default_result["errorInfo"] = chat_result[1]
            return default_result

        payload = chat_result[1]
        if isinstance(payload, dict) and "score" in payload and isinstance(payload["score"], str):
            try:
                payload["score"] = float(payload["score"])
            except ValueError:
                pass
        if isinstance(payload, dict):
            if "errInfo" in payload and "errorInfo" not in payload:
                payload["errorInfo"] = payload.pop("errInfo")
            default_result.update(payload)
        else:
            default_result["result"] = payload
        return default_result
