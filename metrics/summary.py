from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from individualStockReview.core.io import read_excel_records, safe_parse_value, write_excel_records
from individualStockReview.core.logging import get_logger

LOGGER = get_logger("metrics.summary")


def _normalize_number(value: Any, default: float = 0.0) -> float:
    parsed = safe_parse_value(value, default=default)
    if isinstance(parsed, (int, float)):
        return float(parsed)
    try:
        return float(parsed)
    except (TypeError, ValueError):
        return default


def _normalize_list(value: Any) -> list[Any]:
    parsed = safe_parse_value(value, default=[])
    if isinstance(parsed, list):
        return parsed
    if isinstance(parsed, tuple):
        return list(parsed)
    if parsed in (None, ""):
        return []
    return [parsed]


def percision_and_recall(real_file: str, pred_list: list[str], real_list: list[str]) -> tuple[float, float]:
    if len(pred_list) == 0 or len(real_list) == 0:
        return 0.0, 0.0
    pred_true = [pred for pred in pred_list if pred in real_list]
    percision = len(pred_true) / len(pred_list)
    recall = 1.0 if real_file in pred_list else 0.0
    return percision, recall


def percisionAndRecall(realFile, predList, realList):
    return percision_and_recall(realFile, predList, realList)


def recall_performance(pred: list[str], file_name: str) -> int:
    if file_name.replace("《", "").replace("》", "") in pred:
        return 1
    return 0


def recallPerformance(pred, file):
    return recall_performance(pred, file)


def summarize_recall_results(path_list: list[str | Path], real_result_path: str | Path, save_path: str | Path | None = None) -> pd.DataFrame:
    result_list = [read_excel_records(path) for path in path_list]
    real_result_list = read_excel_records(real_result_path)

    mean_result: list[dict[str, dict[str, float]]] = []
    for idx, results in enumerate(result_list):
        percision_list: list[float] = []
        recall_list: list[float] = []
        first_time_list: list[float] = []
        total_time_list: list[float] = []
        for inner_idx, result in enumerate(results):
            pred_file_list = _normalize_list(result.get("实际召回文档"))
            real_file_list = _normalize_list(real_result_list[inner_idx].get("realFile"))
            reason = safe_parse_value(real_result_list[inner_idx].get("queryKeyWord"), default=real_result_list[inner_idx].get("queryKeyWord"))
            file_name = result.get("file") or real_result_list[inner_idx].get("file") or real_result_list[inner_idx].get("fileName")
            percision, recall = percision_and_recall(file_name, pred_file_list, real_file_list)

            result["realFileList"] = real_file_list
            result["reason"] = reason
            result["file"] = file_name
            result["percision"] = percision
            result["recall"] = recall
            result["回答首字耗时"] = _normalize_number(result.get("回答首字耗时"))
            result["总耗时"] = _normalize_number(result.get("总耗时"))

            percision_list.append(percision)
            recall_list.append(recall)
            first_time_list.append(result["回答首字耗时"])
            total_time_list.append(result["总耗时"])

        write_excel_records(results, path_list[idx])
        mean_result.append(
            {
                f"第{idx}次评测结果": {
                    "percision": sum(percision_list) / len(percision_list) if percision_list else 0.0,
                    "recall": sum(recall_list) / len(recall_list) if recall_list else 0.0,
                    "firstTime": sum(first_time_list) / len(first_time_list) if first_time_list else 0.0,
                    "totalTime": sum(total_time_list) / len(total_time_list) if total_time_list else 0.0,
                }
            }
        )

    mean_percision = sum([list(item.values())[0]["percision"] for item in mean_result]) / len(mean_result) if mean_result else 0.0
    mean_recall = sum([list(item.values())[0]["recall"] for item in mean_result]) / len(mean_result) if mean_result else 0.0
    mean_first_time = sum([list(item.values())[0]["firstTime"] for item in mean_result]) / len(mean_result) if mean_result else 0.0
    mean_total_time = sum([list(item.values())[0]["totalTime"] for item in mean_result]) / len(mean_result) if mean_result else 0.0
    mean_result.append(
        {
            "平均评测结果": {
                "percision": mean_percision,
                "recall": mean_recall,
                "firstTime": mean_first_time,
                "totalTime": mean_total_time,
            }
        }
    )

    merged = {}
    for item in mean_result:
        merged.update(item)
    frame = pd.DataFrame(merged, index=list(merged["平均评测结果"].keys()), columns=list(merged.keys())).T
    if save_path:
        write_excel_records(frame, save_path)
    LOGGER.info("平均评测结果 percision=%s recall=%s", mean_percision, mean_recall)
    return frame


def summarize_simple_result(result_path: str | Path, save_back: bool = True) -> pd.DataFrame:
    results = read_excel_records(result_path)
    recall_list: list[float] = []
    first_time_list: list[float] = []
    total_time_list: list[float] = []
    none_num = 0
    for result in results:
        pred_file_list = _normalize_list(result.get("实际召回文档"))
        file_name = result.get("file") or result.get("fileName") or result.get("预期文件") or ""
        recall = recall_performance(pred_file_list, file_name)
        result["回答首字耗时"] = _normalize_number(result.get("回答首字耗时"))
        result["总耗时"] = _normalize_number(result.get("总耗时"))
        result["recall"] = recall
        result["firstTime"] = result["回答首字耗时"]
        result["totalTime"] = result["总耗时"]

        recall_list.append(recall)
        first_time_list.append(result["回答首字耗时"])
        total_time_list.append(result["总耗时"])
        if len(pred_file_list) == 0:
            none_num += 1

    LOGGER.info(
        "评测结果 未召回研报数=%s 空召率=%s recall=%s",
        none_num,
        none_num / len(results) if results else 0.0,
        sum(recall_list) / len(recall_list) if recall_list else 0.0,
    )
    frame = pd.DataFrame(results)
    if save_back:
        write_excel_records(frame, result_path)
    return frame
