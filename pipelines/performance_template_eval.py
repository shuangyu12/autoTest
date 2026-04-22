from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import shutil
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from individualStockReview.agent.registry import AgentRegistry
from individualStockReview.core.config import ConfigManager
from individualStockReview.core.io import safe_parse_value
from individualStockReview.core.logging import get_logger, summarize_data
from individualStockReview.core.paths import PathManager

LOGGER = get_logger("pipelines.performance_template_eval")

REPORT_DATE_TYPE_MAP = {
    "1": "一季报",
    "2": "年报+一季报",
    "3": "半年报",
    "4": "三季报",
    "5": "年报",
}
VALID_SCORES = {0.0, 0.4, 0.7, 1.0}
SYSTEM_PROMPT = """你负责做证券财报业绩模板质检。只判断模板是否合格，不补写、不改写、不解释。输出必须是 JSON 对象，且只包含 `score` 和 `reason` 两个字段。score 只能取 1、0.7、0.4、0。必须独立判断当前输入，不能套用上一次结论；只要仍处于模板态，就不能因为占位符样式、句式差异、前三季度与单季并列、全年与 Q4 并列而判错。"""
PROMPT_TEMPLATE = """
你在评测 `performance_json` 是否适合作为后续自动填充的财报模板。

输入只有两项：
- 报告期：{{ report_period_label }}
- 模板：
{{ performance_json }}

先理解一个合格参考模板（一季报示意）：
- 公司发布{N}年一季报。{N}Q1实现营业总收入-亿元，同比-；实现归母净利润-亿元，同比-；扣非净利润-亿元，同比-。
- 毛利率上，公司{N}Q1实现毛利率-%，同比-pct。费用率上，公司{N}Q1销售/管理费用率分别同比-/-pct至-%；税金及附加费用比率下降至-%，同比-pct。综合来看，{N}Q1公司净利率同比-pct至-%；ROE为-%，同比-pct。
- 现金流方面，公司{N}Q1经营现金流净额-亿元，同比增加-亿元，同比增长-%；销售收现同比-；收现比-%，同比-pct。

判断规则（按类别执行，类别间要明确区分）：
A. 基本判断范围
1. 只看当前模板本身，不要脑补填完数据后的效果，也不要沿用其他样本的结论。

B. 默认允许的正常模板情形
2. 只要还是“占位符”，就默认允许，没有固定搭配要求。`-`、`-%`、`-pct`、`{N}`、`{N+1}`、`{N}Q1`、`{N}Q1-Q3`、`同比XX%`、`同比-%`、`同比-pct`、`同比+%/+%`、`XX%`、`XXpct`、`XX亿元`、`约XX亿元`、`±XX pct` 等都可以，不得因为占位符样式不同就扣分。
3. 以下并列写法默认视为正常，不是重复，更不是错误：`前三季度` 与 `Q3单季` 并列；`全年` 与 `Q4` 并列；`{N}`、`{N}Q4`、`{N+1}Q1` 并列。

C. 必查项：时间表达一致性
4. 时间表达必须和报告期一致。例如一季报模板应围绕 `{N}Q1` / `{N}年一季报`；不能混入年报、半年报、三季报或无关年份季度。年报+一季报模板应围绕 `{N}`、`{N}Q4`、`{N+1}Q1`。

D. 扣分触发条件（只有出现实质问题才扣分）
5. 只有明显未清洗、难以直接填充、报告期错乱、内容自相矛盾时才扣分，例如 `yoy`、`yoy+-%`、`(yoy+-%)`、`同比增长??`、乱码、无关文本、写死真实数值、主观研判结论；像 `12.3%`、`1.26pct`、`123.46亿元` 这种具体阿拉伯数字+单位/百分比属于写死真实数值，不是占位符。

E. `reason` 书写要求
6. 如果你认为有问题，`reason` 必须指出当前模板里的具体问题和对应片段；如果说不出具体问题，就判为合格。不要给出“结构不够精简”“部分重复”“模板不规范”这类空泛结论。

F. 评分档位（四档互斥，优先按问题严重程度归类）
7. `1` 分：模板保持待填状态，结构可用于填充，时间表达正确，且没有实质性错误；这种情况应直接判 1 分。
8. `0.7` 分：只有少量明确问题，且问题局部、可修，不影响整体模板结构识别。
9. `0.4` 分：存在多处明确问题，虽然还能看出模板结构，但已经不能直接使用，需要较多修正。
10. `0` 分：模板基本不可用，或主体内容明显未清洗、严重错期、严重矛盾、无关内容过多，已无法作为可填充模板；尤其当严重错期与无关文本/主观结论/乱码/写死数值等多类问题同时出现时，即使仍能看出部分结构，也应直接判 `0` 分。

只输出一个 JSON 对象：
{
  "score": 1,
  "reason": "用中文简洁说明主要判断依据，不超过120字"
}

要求：
- `score` 只能是 1、0.7、0.4、0
- `reason` 必须具体说明当前模板的合格依据或真实问题点
- 不要出现“模板包含具体数值XX”“违反模板形态要求”“结构不够精简”“部分语句重复啰嗦”这类空泛结论
- 不要输出任何 JSON 之外的内容
""".strip()


class PerformanceTemplateEvalPipeline:
    def __init__(
        self,
        api_file: str | None = None,
        runtime_file: str | None = None,
        volcengine_file: str | None = None,
    ):
        self.config_manager = ConfigManager(api_file=api_file, runtime_file=runtime_file, volcengine_file=volcengine_file)
        self.path_manager = PathManager(runtime_config=self.config_manager.runtime_args)
        self.path_manager.ensure_directories()
        self.registry = AgentRegistry(api_file=api_file, runtime_file=runtime_file, volcengine_file=volcengine_file)
        self.logger = get_logger("pipelines.performance_template_eval")

    @staticmethod
    def _normalize_scalar(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, float) and pd.isna(value):
            return None
        if isinstance(value, str):
            text = value.strip()
            return text if text else None
        return value

    @classmethod
    def _normalize_int_text(cls, value: Any) -> str:
        normalized = cls._normalize_scalar(value)
        if normalized is None:
            return ""
        if isinstance(normalized, bool):
            return str(int(normalized))
        if isinstance(normalized, int):
            return str(normalized)
        if isinstance(normalized, float):
            return str(int(normalized)) if normalized.is_integer() else str(normalized)
        text = str(normalized).strip()
        if text.endswith(".0"):
            try:
                return str(int(float(text)))
            except ValueError:
                return text
        return text

    @staticmethod
    def _extract_year(date_text: str | None) -> int | None:
        if not date_text:
            return None
        match = re.match(r"\s*(\d{4})", str(date_text))
        return int(match.group(1)) if match else None

    @staticmethod
    def _split_report_dates(report_date: Any) -> list[str]:
        if report_date is None:
            return []
        return [part.strip() for part in str(report_date).split("&") if part.strip()]

    @classmethod
    def _build_time_context(cls, report_date: Any, report_date_type: Any) -> dict[str, Any]:
        type_code = cls._normalize_int_text(report_date_type)
        report_dates = cls._split_report_dates(report_date)
        type_name = REPORT_DATE_TYPE_MAP.get(type_code, f"未知类型({type_code})")

        if type_code == "2":
            annual_date = next((item for item in report_dates if item.endswith("12-31")), None)
            q1_date = next((item for item in report_dates if item.endswith("03-31")), None)
            annual_year = cls._extract_year(annual_date)
            q1_year = cls._extract_year(q1_date)
            parsed_years = [year for year in [cls._extract_year(item) for item in report_dates] if year is not None]
            if annual_year is None and parsed_years:
                annual_year = min(parsed_years)
            if q1_year is None and annual_year is not None:
                q1_year = annual_year + 1
            allowed = [
                "{N}",
                "{N+1}",
                "{N}Q4",
                "{N+1}Q1",
                f"{annual_year}年" if annual_year is not None else None,
                f"{annual_year}年年报" if annual_year is not None else None,
                f"{annual_year}年Q4" if annual_year is not None else None,
                f"{q1_year}年Q1" if q1_year is not None else None,
                f"{q1_year}年一季报" if q1_year is not None else None,
            ]
            rule_summary = (
                f"这是年报+一季报模板；年度 N={annual_year if annual_year is not None else '未知'}，"
                f"下一年 Q1={q1_year if q1_year is not None else '未知'}。模板只应围绕 N 年、NQ4、N+1Q1 展开。"
            )
            return {
                "report_date_type_name": type_name,
                "rule_summary": rule_summary,
                "allowed_expressions": [item for item in allowed if item],
                "N": annual_year,
                "N_plus_1": q1_year,
            }

        year = cls._extract_year(report_dates[0]) if report_dates else None
        type_rule_map = {
            "1": {
                "rule_summary": f"这是 {year if year is not None else '未知'} 年一季报模板，只应出现该年度 Q1/一季报相关时间。",
                "allowed_expressions": ["{N}", "{N}Q1", f"{year}年" if year is not None else None, f"{year}年Q1" if year is not None else None, f"{year}年一季报" if year is not None else None],
            },
            "3": {
                "rule_summary": f"这是 {year if year is not None else '未知'} 年半年报模板，只应出现该年度 H1/中报/半年报及 Q2 相关时间。",
                "allowed_expressions": ["{N}", "{N}H1", "{N}Q2", f"{year}年" if year is not None else None, f"{year}年H1" if year is not None else None, f"{year}年半年报" if year is not None else None, f"{year}年中报" if year is not None else None, f"{year}年Q2" if year is not None else None],
            },
            "4": {
                "rule_summary": f"这是 {year if year is not None else '未知'} 年三季报模板，只应出现该年度前三季度/Q1-Q3/Q3 相关时间。",
                "allowed_expressions": ["{N}", "{N}Q1-Q3", "{N}Q3", f"{year}年" if year is not None else None, f"{year}年前三季度" if year is not None else None, f"{year}年Q1-Q3" if year is not None else None, f"{year}年Q3" if year is not None else None, f"{year}年三季报" if year is not None else None],
            },
            "5": {
                "rule_summary": f"这是 {year if year is not None else '未知'} 年年报模板，只应出现该年度全年/年报及 Q4 相关时间。",
                "allowed_expressions": ["{N}", "{N}Q4", f"{year}年" if year is not None else None, f"{year}年Q4" if year is not None else None, f"{year}年年报" if year is not None else None],
            },
        }
        selected = type_rule_map.get(
            type_code,
            {
                "rule_summary": f"报告期类型无法识别，请结合 report_date={report_date} 严格判断时间一致性。",
                "allowed_expressions": report_dates,
            },
        )
        return {
            "report_date_type_name": type_name,
            "rule_summary": selected["rule_summary"],
            "allowed_expressions": [item for item in selected["allowed_expressions"] if item],
            "N": year,
            "N_plus_1": year + 1 if year is not None else None,
        }

    @classmethod
    def _parse_performance_json(cls, raw_value: Any) -> tuple[Any, str]:
        normalized = cls._normalize_scalar(raw_value)
        if normalized is None:
            return [], "[]"
        if isinstance(normalized, (list, dict)):
            return normalized, json.dumps(normalized, ensure_ascii=False, indent=2)
        parsed = safe_parse_value(normalized, default=normalized)
        if isinstance(parsed, (list, dict)):
            return parsed, json.dumps(parsed, ensure_ascii=False, indent=2)
        return normalized, str(normalized)

    @classmethod
    def _build_report_period_label(cls, report_date: Any, report_date_type: Any) -> str:
        type_code = cls._normalize_int_text(report_date_type)
        report_dates = cls._split_report_dates(report_date)
        type_name = REPORT_DATE_TYPE_MAP.get(type_code, "")

        if type_code == "2":
            annual_date = next((item for item in report_dates if item.endswith("12-31")), None)
            q1_date = next((item for item in report_dates if item.endswith("03-31")), None)
            annual_year = cls._extract_year(annual_date)
            q1_year = cls._extract_year(q1_date)
            if annual_year is not None and q1_year is not None:
                return f"{annual_year}年年报+{q1_year}年一季报"
            return type_name or cls._normalize_scalar(report_date) or ""

        year = cls._extract_year(report_dates[0]) if report_dates else None
        suffix_map = {
            "1": "一季报",
            "3": "半年报",
            "4": "三季报",
            "5": "年报",
        }
        suffix = suffix_map.get(type_code)
        if year is not None and suffix:
            return f"{year}年{suffix}"
        return type_name or cls._normalize_scalar(report_date) or ""

    @classmethod
    def _build_template_variables(cls, record: dict[str, Any]) -> dict[str, Any]:
        _, performance_json_text = cls._parse_performance_json(record.get("performance_json"))
        return {
            "report_period_label": cls._build_report_period_label(record.get("report_date"), record.get("report_date_type")),
            "performance_json": performance_json_text,
        }

    @classmethod
    def _record_key(cls, record: dict[str, Any], idx: int) -> str:
        record_id = cls._normalize_int_text(record.get("id"))
        if record_id:
            return record_id
        return f"row-{idx}"

    @classmethod
    def _build_record_digest(cls, record: dict[str, Any], idx: int) -> str:
        fingerprint_payload = {
            "id": cls._record_key(record, idx),
            "security_code": cls._normalize_scalar(record.get("security_code")),
            "report_date": cls._normalize_scalar(record.get("report_date")),
            "report_date_type": cls._normalize_int_text(record.get("report_date_type")),
            "performance_json": cls._normalize_scalar(record.get("performance_json")),
        }
        return hashlib.sha1(json.dumps(fingerprint_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()

    @staticmethod
    def _normalize_score(score: Any) -> float:
        if isinstance(score, str):
            score = score.strip()
        try:
            numeric = float(score)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"无效 score: {score}") from exc
        for valid in VALID_SCORES:
            if abs(numeric - valid) < 1e-9:
                return float(valid)
        raise ValueError(f"score 不在允许集合内: {score}")

    @staticmethod
    def _normalize_reason(reason: Any) -> str:
        text = re.sub(r"\s+", " ", str(reason or "")).strip()
        if not text:
            raise ValueError("模型返回缺少 reason")
        return text

    @staticmethod
    def _normalize_response_text(text: Any) -> str:
        return re.sub(r"\s+", " ", str(text or "")).strip()

    @classmethod
    def _fingerprint_text(cls, text: Any) -> str:
        normalized = cls._normalize_response_text(text)
        return hashlib.sha1(normalized.encode("utf-8")).hexdigest() if normalized else ""

    @classmethod
    def _collect_cache_signals(cls, value: Any, prefix: str = "") -> dict[str, Any]:
        signals: dict[str, Any] = {}
        if isinstance(value, dict):
            for key, item in value.items():
                current_path = f"{prefix}.{key}" if prefix else str(key)
                if "cache" in str(key).lower():
                    signals[current_path] = item
                signals.update(cls._collect_cache_signals(item, current_path))
        elif isinstance(value, list):
            for idx, item in enumerate(value):
                current_path = f"{prefix}[{idx}]" if prefix else f"[{idx}]"
                signals.update(cls._collect_cache_signals(item, current_path))
        return signals

    @classmethod
    def _extract_cache_signals(cls, response: dict[str, Any]) -> dict[str, Any]:
        signals: dict[str, Any] = {}
        for field in ["response_metadata", "usage_metadata", "additional_kwargs"]:
            payload = response.get(field)
            if payload:
                signals.update(cls._collect_cache_signals(payload, field))
        return signals

    @classmethod
    def _value_indicates_cache_hit(cls, value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value > 0
        if isinstance(value, str):
            text = value.strip().lower()
            if not text or text in {"0", "false", "no", "miss", "cache_miss", "none", "null"}:
                return False
            return any(token in text for token in ["hit", "cached", "cache_read", "read", "true"])
        if isinstance(value, dict):
            return any(cls._value_indicates_cache_hit(item) for item in value.values())
        if isinstance(value, list):
            return any(cls._value_indicates_cache_hit(item) for item in value)
        return False

    @classmethod
    def _has_cache_hit(cls, cache_signals: dict[str, Any]) -> bool:
        return any(cls._value_indicates_cache_hit(value) for value in cache_signals.values())

    @classmethod
    def _build_success_result(
        cls,
        record: dict[str, Any],
        idx: int,
        digest: str,
        response: dict[str, Any],
        attempts: int,
    ) -> dict[str, Any]:
        parsed_payload, normalized_performance_json = cls._parse_performance_json(record.get("performance_json"))
        score = cls._normalize_score(response.get("score"))
        reason = cls._normalize_reason(response.get("reason"))
        raw_model_output = str(response.get("result") or "")
        response_metadata = response.get("response_metadata") if isinstance(response.get("response_metadata"), dict) else {}
        usage_metadata = response.get("usage_metadata") if isinstance(response.get("usage_metadata"), dict) else {}
        additional_kwargs = response.get("additional_kwargs") if isinstance(response.get("additional_kwargs"), dict) else {}
        cache_signals = cls._extract_cache_signals(response)
        return {
            "id": cls._record_key(record, idx),
            "idx": idx,
            "security_code": cls._normalize_scalar(record.get("security_code")),
            "report_date": cls._normalize_scalar(record.get("report_date")),
            "report_date_type": cls._normalize_int_text(record.get("report_date_type")),
            "report_date_type_name": REPORT_DATE_TYPE_MAP.get(cls._normalize_int_text(record.get("report_date_type")), "未知类型"),
            "report_period_label": cls._build_report_period_label(record.get("report_date"), record.get("report_date_type")),
            "performance_json": cls._normalize_scalar(record.get("performance_json")),
            "normalized_performance_json": normalized_performance_json,
            "performance_json_parsed": parsed_payload,
            "total_score": score,
            "score_reason": reason,
            "eval_error": "",
            "eval_status": "completed",
            "attempts": attempts,
            "provider": response.get("provider", "volcengine"),
            "model": response.get("model", ""),
            "response_id": str(response.get("response_id") or ""),
            "raw_model_output": raw_model_output,
            "model_output_fingerprint": cls._fingerprint_text(raw_model_output),
            "response_metadata": response_metadata,
            "usage_metadata": usage_metadata,
            "additional_kwargs": additional_kwargs,
            "possible_model_cache_hit": cls._has_cache_hit(cache_signals),
            "cache_signals": cache_signals,
            "suspected_duplicate_output": False,
            "duplicate_output_ids": [],
            "input_digest": digest,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }

    @classmethod
    def _build_failed_result(
        cls,
        record: dict[str, Any],
        idx: int,
        digest: str,
        error_message: str,
        attempts: int,
    ) -> dict[str, Any]:
        parsed_payload, normalized_performance_json = cls._parse_performance_json(record.get("performance_json"))
        return {
            "id": cls._record_key(record, idx),
            "idx": idx,
            "security_code": cls._normalize_scalar(record.get("security_code")),
            "report_date": cls._normalize_scalar(record.get("report_date")),
            "report_date_type": cls._normalize_int_text(record.get("report_date_type")),
            "report_date_type_name": REPORT_DATE_TYPE_MAP.get(cls._normalize_int_text(record.get("report_date_type")), "未知类型"),
            "report_period_label": cls._build_report_period_label(record.get("report_date"), record.get("report_date_type")),
            "performance_json": cls._normalize_scalar(record.get("performance_json")),
            "normalized_performance_json": normalized_performance_json,
            "performance_json_parsed": parsed_payload,
            "total_score": 0.0,
            "score_reason": f"评测调用失败，按不可评估暂记 0 分：{error_message}",
            "eval_error": error_message,
            "eval_status": "failed",
            "attempts": attempts,
            "provider": "volcengine",
            "model": "",
            "response_id": "",
            "raw_model_output": "",
            "model_output_fingerprint": "",
            "response_metadata": {},
            "usage_metadata": {},
            "additional_kwargs": {},
            "possible_model_cache_hit": False,
            "cache_signals": {},
            "suspected_duplicate_output": False,
            "duplicate_output_ids": [],
            "input_digest": digest,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }

    @staticmethod
    def _annotate_response_diagnostics(results: list[dict[str, Any]]) -> dict[str, int]:
        fingerprint_groups: dict[str, list[str]] = {}
        for item in results:
            if item.get("eval_status") != "completed":
                continue
            fingerprint = str(item.get("model_output_fingerprint") or "").strip()
            record_id = str(item.get("id") or "")
            if fingerprint and record_id:
                fingerprint_groups.setdefault(fingerprint, []).append(record_id)

        duplicate_groups = {fingerprint: ids for fingerprint, ids in fingerprint_groups.items() if len(ids) > 1}
        for item in results:
            fingerprint = str(item.get("model_output_fingerprint") or "").strip()
            record_id = str(item.get("id") or "")
            duplicate_ids = [other_id for other_id in duplicate_groups.get(fingerprint, []) if other_id != record_id]
            item["suspected_duplicate_output"] = bool(duplicate_ids)
            item["duplicate_output_ids"] = duplicate_ids

        return {
            "possible_model_cache_hit_records": sum(1 for item in results if item.get("possible_model_cache_hit")),
            "suspected_duplicate_output_records": sum(1 for item in results if item.get("suspected_duplicate_output")),
        }

    @classmethod
    def _build_summary(cls, results: list[dict[str, Any]], total_records: int) -> dict[str, Any]:
        completed_results = [item for item in results if item.get("eval_status") == "completed"]
        failed_results = [item for item in results if item.get("eval_status") != "completed"]
        score_distribution = {"1.0": 0, "0.7": 0, "0.4": 0, "0.0": 0}
        for item in results:
            score_key = f"{float(item.get('total_score', 0.0)):.1f}"
            if score_key in score_distribution:
                score_distribution[score_key] += 1
        average_score = round(
            sum(float(item.get("total_score", 0.0)) for item in completed_results) / len(completed_results),
            4,
        ) if completed_results else 0.0
        diagnostics_summary = cls._annotate_response_diagnostics(results)
        return {
            "total_records": total_records,
            "completed_records": len(completed_results),
            "failed_records": len(failed_results),
            "pending_records": max(total_records - len(results), 0),
            "average_score": average_score,
            "score_distribution": score_distribution,
            **diagnostics_summary,
        }

    @staticmethod
    def _load_json_state(output_path: Path) -> dict[str, Any] | None:
        if not output_path.exists():
            return None
        try:
            with output_path.open("r", encoding="utf-8") as file:
                payload = json.load(file)
            return payload if isinstance(payload, dict) else None
        except Exception as exc:
            LOGGER.warning("读取历史结果失败，将忽略旧文件: %s", exc)
            return None

    @staticmethod
    def _build_internal_state_path(output_path: Path) -> Path:
        if output_path.suffix:
            internal_name = f"{output_path.stem}.state{output_path.suffix}"
        else:
            internal_name = f"{output_path.name}.state.json"
        return output_path.with_name(internal_name)

    @staticmethod
    def _atomic_write_json(output_path: Path, data: dict[str, Any]) -> None:
        target = PathManager.ensure_parent(output_path)
        temp_path = target.with_suffix(f"{target.suffix}.tmp")
        with temp_path.open("w", encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False, indent=2)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temp_path, target)

    @staticmethod
    def _build_chain_status(results: list[dict[str, Any]], total_records: int) -> tuple[list[str], str]:
        issues: list[str] = []
        failed_results = [item for item in results if item.get("eval_status") != "completed"]
        pending_records = max(total_records - len(results), 0)

        if total_records == 0:
            issues.append("未读取到可评测记录")
        if failed_results:
            issues.append(f"有 {len(failed_results)} 条记录评测失败")
        if pending_records:
            issues.append(f"有 {pending_records} 条记录尚未完成")

        if not issues:
            return [], ""
        if total_records == 0:
            return issues, "评测链路未读取到可用输入数据"
        if pending_records:
            return issues, f"评测链路未完全完成，仍有 {pending_records} 条记录未产出结果"

        sample_errors = [
            f"{item.get('id')}: {item.get('eval_error')}"
            for item in failed_results[:3]
            if item.get("eval_error")
        ]
        error_info = f"评测链路已完成，但有 {len(failed_results)} 条记录评测失败"
        if sample_errors:
            error_info = f"{error_info}；示例：{'；'.join(sample_errors)}"
        return issues, error_info

    def _build_state_payload(
        self,
        data_path: str,
        output_path: Path,
        provider_config: dict[str, Any],
        result_map: dict[str, dict[str, Any]],
        ordered_keys: list[str],
        total_records: int,
        resumed_records: int,
        requested_records: int | None,
        run_mode: str,
    ) -> dict[str, Any]:
        ordered_results = [result_map[key] for key in ordered_keys if key in result_map]
        issues, error_info = self._build_chain_status(ordered_results, total_records=total_records)
        return {
            "meta": {
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "data_path": data_path,
                "output_path": str(output_path),
                "provider": provider_config.get("provider", "volcengine"),
                "model": provider_config.get("model", ""),
                "sdk_max_retries": provider_config.get("max_retries", 0),
                "resumed_records": resumed_records,
                "requested_records": requested_records,
                "run_mode": run_mode,
            },
            "summary": self._build_summary(ordered_results, total_records=total_records),
            "issues": issues,
            "errorInfo": error_info,
            "results": ordered_results,
        }

    @classmethod
    def _build_public_result(cls, item: dict[str, Any]) -> dict[str, Any]:
        template_value = cls._normalize_scalar(item.get("normalized_performance_json"))
        if template_value is None:
            _, template_value = cls._parse_performance_json(item.get("performance_json"))
        report_period_label = cls._normalize_scalar(item.get("report_period_label"))
        if report_period_label is None:
            report_period_label = cls._build_report_period_label(item.get("report_date"), item.get("report_date_type"))
        return {
            "id": cls._normalize_scalar(item.get("id")),
            "个股代码": cls._normalize_scalar(item.get("security_code")) or "",
            "报告期": report_period_label or "",
            "模板": template_value or "",
            "评分": float(item.get("total_score", 0.0)),
            "评分原因": cls._normalize_scalar(item.get("score_reason")) or "",
            "isSucess": item.get("eval_status") == "completed",
            "errorInfo": cls._normalize_scalar(item.get("eval_error")) or "",
        }

    @classmethod
    def _build_public_state_payload(cls, state_payload: dict[str, Any]) -> dict[str, Any]:
        return {
            "meta": dict(state_payload.get("meta") or {}),
            "summary": dict(state_payload.get("summary") or {}),
            "issues": list(state_payload.get("issues") or []),
            "errorInfo": state_payload.get("errorInfo") or "",
            "results": [
                cls._build_public_result(item)
                for item in (state_payload.get("results") or [])
                if isinstance(item, dict)
            ],
        }

    @staticmethod
    def _excel_safe_value(value: Any) -> Any:
        if value is None:
            return ""
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        return value

    def _export_excel(self, state_payload: dict[str, Any], json_output_path: Path) -> Path:
        excel_path = PathManager.ensure_parent(json_output_path.with_suffix(".xlsx"))
        overview_row = {
            **{f"meta_{key}": self._excel_safe_value(value) for key, value in (state_payload.get("meta") or {}).items()},
            **{f"summary_{key}": self._excel_safe_value(value) for key, value in (state_payload.get("summary") or {}).items()},
            "errorInfo": self._excel_safe_value(state_payload.get("errorInfo")),
            "issues": self._excel_safe_value(state_payload.get("issues") or []),
        }
        result_rows = [
            {key: self._excel_safe_value(value) for key, value in item.items()}
            for item in (state_payload.get("results") or [])
            if isinstance(item, dict)
        ]
        issues_rows = [{"issue": issue} for issue in (state_payload.get("issues") or [])]

        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            pd.DataFrame([overview_row]).to_excel(writer, sheet_name="overview", index=False)
            pd.DataFrame(result_rows).to_excel(writer, sheet_name="results", index=False)
            pd.DataFrame(issues_rows).to_excel(writer, sheet_name="issues", index=False)

        try:
            from openpyxl import load_workbook
            from openpyxl.styles import Font

            workbook = load_workbook(excel_path)
            base_font = Font(name="Arial", size=11)
            header_font = Font(name="Arial", size=11, bold=True)
            for sheet in workbook.worksheets:
                for row_idx, row in enumerate(sheet.iter_rows(), start=1):
                    for cell in row:
                        cell.font = header_font if row_idx == 1 else base_font
            workbook.save(excel_path)
        except Exception as exc:
            self.logger.warning("Excel 样式写入失败，已保留数据文件: %s", exc)

        mirror_path = self.path_manager.build_compat_mirror_path(excel_path)
        if mirror_path and mirror_path.resolve(strict=False) != excel_path.resolve(strict=False):
            shutil.copy2(excel_path, mirror_path)
        return excel_path

    @classmethod
    def read_csv_records(cls, data_path: str | Path, limit: int | None = None) -> list[dict[str, Any]]:
        frame = pd.read_csv(data_path)
        frame = frame.where(pd.notna(frame), None)
        records = [frame.iloc[idx, :].to_dict() for idx in range(len(frame))]
        return records[:limit] if limit is not None else records

    async def _evaluate_single(
        self,
        evaluator,
        record: dict[str, Any],
        idx: int,
        prompt_template: str,
        system_prompt: str,
        keep_missing: bool,
    ) -> dict[str, Any]:
        digest = self._build_record_digest(record, idx)
        template_variables = self._build_template_variables(record)
        default_result = {
            "idx": idx,
            "id": self._record_key(record, idx),
            "messages": "",
            "result": "",
            "score": "",
            "reason": "",
        }
        response = await evaluator.evaluate(
            default_result=deepcopy(default_result),
            prompt_template=prompt_template,
            system_prompt=system_prompt,
            template_variables=template_variables,
            parse_json=True,
            keep_missing=keep_missing,
        )
        if response.get("isSucess"):
            try:
                return self._build_success_result(record=record, idx=idx, digest=digest, response=response, attempts=1)
            except Exception as exc:
                error_message = f"模型输出格式校验失败: {exc}"
                self.logger.warning("评测结果格式异常，直接记失败: idx=%s error=%s", idx, exc)
                return self._build_failed_result(record=record, idx=idx, digest=digest, error_message=error_message, attempts=1)

        error_message = str(response.get("errInfo") or "模型调用失败")
        self.logger.warning("评测调用失败，直接记失败: idx=%s error=%s", idx, error_message)
        return self._build_failed_result(record=record, idx=idx, digest=digest, error_message=error_message, attempts=1)

    async def evaluate_async(
        self,
        data_path: str,
        save_path: str | None = None,
        parallel_num: int = 5,
        max_eval_retries: int = 1,
        retry_interval: float = 0.0,
        save_every: int = 1,
        limit: int | None = None,
        prompt_template: str = PROMPT_TEMPLATE,
        system_prompt: str = SYSTEM_PROMPT,
        provider_config: dict[str, Any] | None = None,
        keep_missing: bool = True,
        resume: bool = True,
    ) -> dict[str, Any]:
        parallel_num = max(int(parallel_num), 1)
        save_every = max(int(save_every), 1)
        limit = max(int(limit), 1) if limit is not None else None
        run_mode = "resume" if resume else "overwrite"
        output_path = Path(save_path) if save_path else self.path_manager.output_path("performance_template_eval.json")
        internal_output_path = self._build_internal_state_path(output_path)
        provider_payload = self.registry.clone_provider_config("volcengine")
        if provider_config:
            provider_payload.update(deepcopy(provider_config))
        provider_payload["max_retries"] = 0
        evaluator = self.registry.get_volcengine_evaluator(override_config=provider_payload)

        records = self.read_csv_records(data_path=data_path, limit=limit)
        ordered_keys = [self._record_key(record, idx) for idx, record in enumerate(records)]
        total_records = len(records)
        result_map: dict[str, dict[str, Any]] = {}
        resumed_records = 0

        if resume:
            existing_state = self._load_json_state(internal_output_path) or self._load_json_state(output_path)
            if existing_state and isinstance(existing_state.get("results"), list):
                for item in existing_state["results"]:
                    if not isinstance(item, dict) or not item.get("id"):
                        continue
                    if "eval_status" not in item or "input_digest" not in item:
                        continue
                    result_map[str(item["id"])] = item
                resumed_records = len([item for item in result_map.values() if item.get("eval_status") == "completed"])

        save_lock = asyncio.Lock()
        semaphore = asyncio.Semaphore(parallel_num)
        save_counter = {"value": 0}

        async def persist_state(force: bool = False) -> None:
            async with save_lock:
                if not force and save_every > 1 and save_counter["value"] < save_every:
                    return
                state_payload = self._build_state_payload(
                    data_path=data_path,
                    output_path=output_path,
                    provider_config=provider_payload,
                    result_map=result_map,
                    ordered_keys=ordered_keys,
                    total_records=total_records,
                    resumed_records=resumed_records,
                    requested_records=limit,
                    run_mode=run_mode,
                )
                public_state = self._build_public_state_payload(state_payload)
                self._atomic_write_json(internal_output_path, state_payload)
                self._atomic_write_json(output_path, public_state)
                save_counter["value"] = 0

        async def worker(idx: int, record: dict[str, Any]) -> None:
            key = self._record_key(record, idx)
            digest = self._build_record_digest(record, idx)
            existing = result_map.get(key)
            if resume and existing and existing.get("eval_status") == "completed" and existing.get("input_digest") == digest:
                self.logger.debug("命中断点续跑记录，跳过: %s", summarize_data({"id": key, "idx": idx}))
                return
            async with semaphore:
                result = await self._evaluate_single(
                    evaluator=evaluator,
                    record=record,
                    idx=idx,
                    prompt_template=prompt_template,
                    system_prompt=system_prompt,
                    keep_missing=keep_missing,
                )
            result_map[key] = result
            save_counter["value"] += 1
            await persist_state(force=save_every <= 1)

        tasks = [worker(idx, deepcopy(record)) for idx, record in enumerate(records)]
        if tasks:
            await asyncio.gather(*tasks)
        await persist_state(force=True)
        final_state = self._build_state_payload(
            data_path=data_path,
            output_path=output_path,
            provider_config=provider_payload,
            result_map=result_map,
            ordered_keys=ordered_keys,
            total_records=total_records,
            resumed_records=resumed_records,
            requested_records=limit,
            run_mode=run_mode,
        )
        public_state = self._build_public_state_payload(final_state)
        excel_path = self._export_excel(public_state, output_path)
        final_state.setdefault("meta", {})["excel_output_path"] = str(excel_path)
        public_state.setdefault("meta", {})["excel_output_path"] = str(excel_path)
        self._atomic_write_json(internal_output_path, final_state)
        self._atomic_write_json(output_path, public_state)
        self.logger.info("performance_json 模板评测完成: %s", summarize_data(public_state.get("summary", {})))
        return public_state

    def evaluate(self, *args, **kwargs) -> dict[str, Any]:
        return asyncio.run(self.evaluate_async(*args, **kwargs))


def _build_provider_config_from_args(args: argparse.Namespace) -> dict[str, Any]:
    provider_config: dict[str, Any] = {}
    for key in ["provider", "base_url", "api_key", "model", "temperature", "timeout", "max_retries"]:
        value = getattr(args, key, None)
        if value is not None:
            provider_config[key] = value
    return provider_config


def _resolve_resume_flag(args: argparse.Namespace) -> bool:
    if getattr(args, "disable_resume", False):
        return False
    return getattr(args, "run_mode", "resume") != "overwrite"


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="基于火山 Ark + LangChain 的 performance_json 模板评测脚本")
    parser.add_argument("--data-path", required=True, help="CSV 数据源路径，例如 /home/customTest/模板数据汇总.csv")
    parser.add_argument("--output", default="", help="输出 JSON 文件路径，默认写入 outputs/performance_template_eval.json")
    parser.add_argument("--parallel-num", type=int, default=5, help="并发评测数")
    parser.add_argument("--max-eval-retries", type=int, default=1, help="兼容旧参数，已停用；当前脚本固定单次请求，不重试")
    parser.add_argument("--retry-interval", type=float, default=0.0, help="兼容旧参数，已停用；当前脚本固定不等待重试")
    parser.add_argument("--save-every", type=int, default=1, help="每完成多少条结果落盘一次，默认 1 表示实时保存")
    parser.add_argument("--use-count", "--limit", dest="use_count", type=int, default=None, help="本次使用的数据量，只评测前 N 条；默认全部")
    parser.add_argument("--run-mode", choices=["resume", "overwrite"], default="resume", help="运行模式：resume=中断恢复并保留历史结果，overwrite=重新开始并覆盖历史结果")
    parser.add_argument("--api-file", default=None, help="apiInfo.yaml 路径")
    parser.add_argument("--runtime-file", default=None, help="runtime.yaml 路径")
    parser.add_argument("--volcengine-file", default=None, help="volcengine.yaml 路径")
    parser.add_argument("--provider", default=None, help="provider 名称，默认 volcengine")
    parser.add_argument("--base-url", default=None, help="火山 Ark OpenAI 兼容地址")
    parser.add_argument("--api-key", default=None, help="火山 Ark API Key")
    parser.add_argument("--model", default=None, help="火山模型 ID")
    parser.add_argument("--temperature", type=float, default=None, help="模型温度")
    parser.add_argument("--timeout", type=float, default=None, help="模型超时时间")
    parser.add_argument("--max-retries", type=int, default=None, help="兼容旧参数；当前脚本内部固定关闭底层 SDK 重试")
    parser.add_argument("--thinking-mode", choices=["auto", "enabled", "disabled"], default=None, help="火山模型思考模式：disabled 表示关闭深度思考，auto 表示沿用配置")
    parser.add_argument("--disable-resume", action="store_true", help="兼容旧参数：等价于 --run-mode overwrite")
    return parser


if __name__ == "__main__":
    arguments = build_argument_parser().parse_args()
    pipeline = PerformanceTemplateEvalPipeline(
        api_file=arguments.api_file,
        runtime_file=arguments.runtime_file,
        volcengine_file=arguments.volcengine_file,
    )
    result = pipeline.evaluate(
        data_path=arguments.data_path,
        save_path=arguments.output or None,
        parallel_num=max(arguments.parallel_num, 1),
        max_eval_retries=max(arguments.max_eval_retries, 1),
        retry_interval=max(arguments.retry_interval, 0.0),
        save_every=max(arguments.save_every, 1),
        limit=arguments.use_count,
        provider_config=_build_provider_config_from_args(arguments),
        resume=_resolve_resume_flag(arguments),
    )
    print(json.dumps(result.get("summary", {}), ensure_ascii=False, indent=2))
