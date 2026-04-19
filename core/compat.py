from __future__ import annotations

from copy import deepcopy
from typing import Any


def build_query_result() -> dict[str, Any]:
    return {
        "query": "",
        "reason": "",
        "fileId": "",
        "fileName": "",
        "isSucess": False,
        "errInfo": "",
    }


def build_select_query_result() -> dict[str, Any]:
    return {
        "idx": "",
        "messages": "",
        "fileList": "",
        "queryKeyWord": "",
        "realFile": "",
        "score": "",
        "reason": "",
        "isSucess": False,
        "errInfo": "",
    }


def build_quote_result() -> dict[str, Any]:
    return {
        "idx": "",
        "messages": "",
        "result": "",
        "score": "",
        "firstResponseTime": 0.0,
        "totalResponseTime": 0.0,
        "isSucess": False,
        "errInfo": "",
    }


def build_answer_score_result() -> dict[str, Any]:
    return {
        "idx": "",
        "messages": "",
        "result": "",
        "score": "",
        "reason": "",
        "isSucess": False,
        "errInfo": "",
    }


def build_data_prepare_result(include_file_paths: bool = False) -> dict[str, Any]:
    payload = {
        "security_code": None,
        "stockName": "",
        "firstPortId": None,
        "secondPortId": None,
    }
    if include_file_paths:
        payload.update({"firstFilePath": None, "secondFilePath": None})
    return payload


def build_model_eval_result() -> dict[str, Any]:
    return {
        "messages": "",
        "mergedTemplate": "",
        "isSucess": True,
        "errInfo": "",
        "chatResult": "",
        "score": 0.0,
        "reason": "",
    }


def build_langchain_eval_result() -> dict[str, Any]:
    return {
        "idx": "",
        "messages": "",
        "result": "",
        "score": "",
        "reason": "",
        "provider": "volcengine",
        "model": "",
        "isSucess": False,
        "errInfo": "",
    }


def ensure_legacy_defaults(result: dict[str, Any], template: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(template)
    merged.update(result)
    return merged


def coerce_score(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return value
    return value
