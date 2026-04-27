from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import time
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from tqdm import tqdm

from individualStockReview.agent.registry import AgentRegistry
from individualStockReview.core.io import safe_parse_value
from individualStockReview.core.logging import get_logger
from individualStockReview.pipelines.prompts.performance_template_eval_prompts import (
    SYSTEM_PROMPT,
    USER_PROMPT_TEMPLATE,
)
from individualStockReview.utils.base import retryClass

LOGGER = get_logger("pipelines.performance_template_eval")
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_PATH = str(PROJECT_ROOT / "outputs" / "performance_template_eval.json")
DEFAULT_PARALLEL_NUM = 5
DEFAULT_RETRY_NUM = 3
DEFAULT_REQUEST_DELAY = 0.0
VALID_SCORES = {0.0, 0.4, 0.7, 1.0}
REPORT_DATE_TYPE_MAP = {
    "1": "一季报",
    "2": "年报+一季报",
    "3": "半年报",
    "4": "三季报",
    "5": "年报",
}


class PerformanceTemplateEvalPipeline:
    def __init__(
        self,
        api_file: str | None = None,
        runtime_file: str | None = None,
        volcengine_file: str | None = None,
    ):
        self.registry = AgentRegistry(api_file=api_file, runtime_file=runtime_file, volcengine_file=volcengine_file)
        self.logger = LOGGER

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
        return record_id or f"row-{idx}"

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

    @classmethod
    def _normalize_manual_score(cls, value: Any) -> Any:
        normalized = cls._normalize_scalar(value)
        if normalized is None:
            return ""
        try:
            return cls._normalize_score(normalized)
        except ValueError:
            return normalized

    @classmethod
    def _build_result_row(
        cls,
        record: dict[str, Any],
        idx: int,
        *,
        score: Any = "",
        reason: str = "",
        is_success: bool = False,
        error_info: str = "",
        badcase_validation: bool = False,
    ) -> dict[str, Any]:
        row = {
            "id": cls._record_key(record, idx),
            "security_code": cls._normalize_scalar(record.get("security_code")) or "",
            "report_date": cls._normalize_scalar(record.get("report_date")) or "",
            "report_date_type": cls._normalize_int_text(record.get("report_date_type")),
            "performance_json": cls._normalize_scalar(record.get("performance_json")) or "",
            "score": score,
            "reason": reason,
            "isSucess": is_success,
            "errorInfo": error_info,
        }
        if badcase_validation:
            row["manual_score"] = cls._normalize_manual_score(record.get("manual_score"))
        return row

    @staticmethod
    def _load_state(output_path: Path) -> dict[str, dict[str, Any]]:
        if not output_path.exists():
            return {}
        try:
            with output_path.open("r", encoding="utf-8") as file:
                payload = json.load(file)
        except Exception as exc:
            LOGGER.warning("读取历史结果失败，将忽略旧结果: %s", exc)
            return {}

        rows = payload.get("results", []) if isinstance(payload, dict) else payload
        if not isinstance(rows, list):
            return {}

        state: dict[str, dict[str, Any]] = {}
        for idx, item in enumerate(rows):
            if not isinstance(item, dict):
                continue
            key = str(item.get("id") or f"row-{idx}")
            state[key] = item
        return state

    @staticmethod
    def _rows_in_order(result_map: dict[str, dict[str, Any]], ordered_keys: list[str]) -> list[dict[str, Any]]:
        return [result_map[key] for key in ordered_keys if key in result_map]

    @staticmethod
    def _atomic_write_json(output_path: Path, payload: dict[str, Any]) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = output_path.with_suffix(f"{output_path.suffix}.tmp")
        with temp_path.open("w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False, indent=2)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temp_path, output_path)

    @staticmethod
    def _should_run(existing: dict[str, Any] | None, resume_type: str) -> bool:
        if resume_type == "unfinished":
            return existing is None
        if resume_type == "errors":
            return existing is not None and not bool(existing.get("isSucess"))
        return existing is None or not bool(existing.get("isSucess"))

    @staticmethod
    async def _wait_for_request_slot(request_delay: float, delay_lock: asyncio.Lock, request_state: dict[str, float]) -> None:
        if request_delay <= 0:
            return
        async with delay_lock:
            now = time.monotonic()
            last_started = request_state.get("last_started", 0.0)
            wait_seconds = max(0.0, request_delay - (now - last_started)) if last_started else 0.0
            if wait_seconds > 0:
                await asyncio.sleep(wait_seconds)
            request_state["last_started"] = time.monotonic()

    async def _evaluate_once(
        self,
        evaluator,
        record: dict[str, Any],
        idx: int,
        request_delay: float,
        delay_lock: asyncio.Lock,
        request_state: dict[str, float],
    ) -> tuple[float, str]:
        await self._wait_for_request_slot(request_delay, delay_lock, request_state)
        response = await evaluator.evaluate(
            default_result={
                "id": self._record_key(record, idx),
                "messages": "",
                "result": "",
                "score": "",
                "reason": "",
            },
            prompt_template=USER_PROMPT_TEMPLATE,
            system_prompt=SYSTEM_PROMPT,
            template_variables=self._build_template_variables(record),
            parse_json=True,
            keep_missing=True,
        )
        if not isinstance(response, dict) or not response.get("isSucess"):
            error_info = "模型调用失败"
            if isinstance(response, dict):
                error_info = str(response.get("errorInfo") or error_info)
            raise RuntimeError(error_info)
        return self._normalize_score(response.get("score")), self._normalize_reason(response.get("reason"))

    async def _evaluate_single(
        self,
        evaluator,
        record: dict[str, Any],
        idx: int,
        request_delay: float,
        retry_num: int,
        retry_interval: float,
        delay_lock: asyncio.Lock,
        request_state: dict[str, float],
        badcase_validation: bool,
    ) -> dict[str, Any]:
        retry_result = await retryClass.decorator(
            self._evaluate_once,
            None,
            False,
            max(int(retry_num), 1),
            max(float(retry_interval), 0.0),
            evaluator,
            record,
            idx,
            request_delay,
            delay_lock,
            request_state,
        )
        if retry_result[0]:
            score, reason = retry_result[1]
            return self._build_result_row(
                record,
                idx,
                score=score,
                reason=reason,
                is_success=True,
                error_info="",
                badcase_validation=badcase_validation,
            )
        return self._build_result_row(
            record,
            idx,
            score="",
            reason="",
            is_success=False,
            error_info=str(retry_result[1]),
            badcase_validation=badcase_validation,
        )

    @staticmethod
    def _build_summary(results: list[dict[str, Any]], total_records: int) -> dict[str, Any]:
        completed_rows = [row for row in results if row.get("isSucess")]
        failed_rows = [row for row in results if not row.get("isSucess")]
        valid_scores = []
        for row in completed_rows:
            try:
                valid_scores.append(float(row.get("score")))
            except (TypeError, ValueError):
                continue
        average_score = round(sum(valid_scores) / len(valid_scores), 4) if valid_scores else 0.0
        return {
            "total_records": total_records,
            "completed_records": len(completed_rows),
            "failed_records": len(failed_rows),
            "pending_records": max(total_records - len(results), 0),
            "average_score": average_score,
        }

    def _build_payload(
        self,
        *,
        data_path: str,
        output_path: Path,
        provider_config: dict[str, Any],
        result_map: dict[str, dict[str, Any]],
        ordered_keys: list[str],
        total_records: int,
        run_mode: str,
        resume_type: str,
        badcase_validation: bool,
    ) -> dict[str, Any]:
        rows = self._rows_in_order(result_map, ordered_keys)
        return {
            "meta": {
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "data_path": data_path,
                "output_path": str(output_path),
                "provider": provider_config.get("provider", "volcengine"),
                "model": provider_config.get("model", ""),
                "run_mode": run_mode,
                "resume_type": resume_type,
                "badcase_validation": badcase_validation,
            },
            "summary": self._build_summary(rows, total_records=total_records),
            "results": rows,
        }

    @staticmethod
    def _excel_safe_value(value: Any) -> Any:
        if value is None:
            return ""
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        return value

    def _export_xlsx(self, payload: dict[str, Any], output_path: Path) -> Path | None:
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            excel_path = output_path.with_suffix(".xlsx")
            overview_row = {
                **{f"meta_{key}": self._excel_safe_value(value) for key, value in (payload.get("meta") or {}).items()},
                **{f"summary_{key}": self._excel_safe_value(value) for key, value in (payload.get("summary") or {}).items()},
            }
            rows = [
                {key: self._excel_safe_value(value) for key, value in row.items()}
                for row in (payload.get("results") or [])
                if isinstance(row, dict)
            ]
            with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
                pd.DataFrame([overview_row]).to_excel(writer, sheet_name="overview", index=False)
                pd.DataFrame(rows).to_excel(writer, sheet_name="results", index=False)
            return excel_path
        except Exception as exc:
            self.logger.warning("导出 Excel 失败，已保留 JSON 结果: %s", exc)
            return None

    @classmethod
    def read_csv_records(cls, data_path: str | Path, limit: int | None = None) -> list[dict[str, Any]]:
        frame = pd.read_csv(data_path)
        frame = frame.where(pd.notna(frame), None)
        records = [frame.iloc[idx, :].to_dict() for idx in range(len(frame))]
        return records[:limit] if limit is not None else records

    async def evaluate_async(
        self,
        data_path: str,
        save_path: str | None = None,
        limit: int | None = None,
        run_mode: str = "resume",
        resume_type: str = "all",
        parallel_num: int = DEFAULT_PARALLEL_NUM,
        request_delay: float = DEFAULT_REQUEST_DELAY,
        retry_num: int = DEFAULT_RETRY_NUM,
        retry_interval: float = 0.0,
        provider_config: dict[str, Any] | None = None,
        badcase_validation: bool = False,
    ) -> dict[str, Any]:
        if not str(data_path or "").strip():
            raise ValueError("data_path 不能为空，请通过参数传入输入 CSV 文件路径")

        output_path = Path(save_path or DEFAULT_OUTPUT_PATH)
        records = self.read_csv_records(data_path=data_path, limit=limit)
        ordered_keys = [self._record_key(record, idx) for idx, record in enumerate(records)]
        total_records = len(records)
        result_map = {} if run_mode == "overwrite" else self._load_state(output_path)

        provider_payload = self.registry.clone_provider_config("volcengine")
        if provider_config:
            provider_payload.update(deepcopy(provider_config))
        provider_payload["max_retries"] = 0
        evaluator = self.registry.get_volcengine_evaluator(override_config=provider_payload)

        semaphore = asyncio.Semaphore(max(int(parallel_num), 1))
        save_lock = asyncio.Lock()
        progress_lock = asyncio.Lock()
        delay_lock = asyncio.Lock()
        request_state = {"last_started": 0.0}
        progress = {"done": 0, "success": 0, "failed": 0, "skipped": 0}
        progress_bar = tqdm(total=total_records, desc="performance模板评测", unit="条", dynamic_ncols=True)

        async def persist() -> None:
            async with save_lock:
                payload = self._build_payload(
                    data_path=data_path,
                    output_path=output_path,
                    provider_config=provider_payload,
                    result_map=result_map,
                    ordered_keys=ordered_keys,
                    total_records=total_records,
                    run_mode=run_mode,
                    resume_type=resume_type,
                    badcase_validation=badcase_validation,
                )
                self._atomic_write_json(output_path, payload)

        async def advance_progress(status: str) -> None:
            async with progress_lock:
                progress["done"] += 1
                if status in progress:
                    progress[status] += 1
                progress_bar.set_postfix(
                    success=progress["success"],
                    failed=progress["failed"],
                    skipped=progress["skipped"],
                    refresh=False,
                )
                progress_bar.update(1)

        async def worker(idx: int, record: dict[str, Any]) -> None:
            key = self._record_key(record, idx)
            if run_mode != "overwrite" and not self._should_run(result_map.get(key), resume_type):
                await advance_progress("skipped")
                return
            async with semaphore:
                result = await self._evaluate_single(
                    evaluator=evaluator,
                    record=record,
                    idx=idx,
                    request_delay=max(float(request_delay), 0.0),
                    retry_num=max(int(retry_num), 1),
                    retry_interval=max(float(retry_interval), 0.0),
                    delay_lock=delay_lock,
                    request_state=request_state,
                    badcase_validation=badcase_validation,
                )
            result_map[key] = result
            await persist()
            await advance_progress("success" if result.get("isSucess") else "failed")

        try:
            tasks = [worker(idx, deepcopy(record)) for idx, record in enumerate(records)]
            if tasks:
                await asyncio.gather(*tasks)
            await persist()
        finally:
            progress_bar.close()

        payload = self._build_payload(
            data_path=data_path,
            output_path=output_path,
            provider_config=provider_payload,
            result_map=result_map,
            ordered_keys=ordered_keys,
            total_records=total_records,
            run_mode=run_mode,
            resume_type=resume_type,
            badcase_validation=badcase_validation,
        )
        excel_path = self._export_xlsx(payload, output_path)
        if excel_path is not None:
            payload.setdefault("meta", {})["excel_output_path"] = str(excel_path)
        self._atomic_write_json(output_path, payload)
        self.logger.info("performance 模板评测完成，结果条数=%s", len(payload.get("results", [])))
        return payload

    def evaluate(self, *args, **kwargs) -> dict[str, Any]:
        return asyncio.run(self.evaluate_async(*args, **kwargs))


def _build_provider_config_from_args(args: argparse.Namespace) -> dict[str, Any]:
    provider_config: dict[str, Any] = {}
    for key in ["provider", "base_url", "api_key", "model", "temperature", "timeout"]:
        value = getattr(args, key, None)
        if value is not None:
            provider_config[key] = value
    return provider_config


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="基于火山模型的 performance_json 模板评测脚本")
    parser.add_argument("--data-path", required=True, help="输入 CSV 文件路径")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_PATH, help="实时保存的 JSON 输出路径，最终会生成同名 xlsx")
    parser.add_argument("--use-count", "--limit", dest="use_count", type=int, default=None, help="只处理前 N 条数据")
    parser.add_argument("--parallel-num", type=int, default=DEFAULT_PARALLEL_NUM, help="并发评测数")
    parser.add_argument("--request-delay", type=float, default=DEFAULT_REQUEST_DELAY, help="不同请求启动之间的最小间隔秒数")
    parser.add_argument("--retry-num", type=int, default=DEFAULT_RETRY_NUM, help="单条评测失败后的最大重试次数")
    parser.add_argument("--retry-interval", type=float, default=0.0, help="单条评测失败后，两次重试之间的等待秒数")
    parser.add_argument(
        "--run-mode",
        choices=["resume", "overwrite"],
        default="resume",
        help="resume=基于历史 JSON 续跑，overwrite=忽略历史结果重跑",
    )
    parser.add_argument(
        "--resume-type",
        choices=["all", "unfinished", "errors"],
        default="all",
        help="all=补跑缺失和失败；unfinished=只补跑缺失；errors=只重跑失败",
    )
    parser.add_argument("--badcase-validation", action="store_true", help="开启 badcase 模式：输入和输出额外保留 manual_score")
    parser.add_argument("--api-file", default=None, help="apiInfo.yaml 路径")
    parser.add_argument("--runtime-file", default=None, help="runtime.yaml 路径")
    parser.add_argument("--volcengine-file", default=None, help="volcengine.yaml 路径")
    parser.add_argument("--provider", default=None, help="provider 名称，默认 volcengine")
    parser.add_argument("--base-url", default=None, help="火山 Ark OpenAI 兼容地址")
    parser.add_argument("--api-key", default=None, help="火山 Ark API Key")
    parser.add_argument("--model", default=None, help="火山模型 ID")
    parser.add_argument("--temperature", type=float, default=None, help="模型温度")
    parser.add_argument("--timeout", type=float, default=None, help="模型超时时间")
    return parser


def main(args: argparse.Namespace | None = None) -> dict[str, Any]:
    arguments = args or build_argument_parser().parse_args()
    pipeline = PerformanceTemplateEvalPipeline(
        api_file=arguments.api_file,
        runtime_file=arguments.runtime_file,
        volcengine_file=arguments.volcengine_file,
    )
    return pipeline.evaluate(
        data_path=arguments.data_path,
        save_path=arguments.output,
        limit=arguments.use_count,
        run_mode=arguments.run_mode,
        resume_type=arguments.resume_type,
        parallel_num=arguments.parallel_num,
        request_delay=arguments.request_delay,
        retry_num=arguments.retry_num,
        retry_interval=arguments.retry_interval,
        provider_config=_build_provider_config_from_args(arguments),
        badcase_validation=bool(arguments.badcase_validation),
    )


if __name__ == "__main__":
    result = main()
    print(json.dumps(result.get("summary", {}), ensure_ascii=False, indent=2))
