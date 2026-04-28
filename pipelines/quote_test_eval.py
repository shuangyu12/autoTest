from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from copy import deepcopy
from pathlib import Path
from typing import Any

import pandas as pd
from tqdm import tqdm

from individualStockReview.agent.registry import AgentRegistry
from individualStockReview.core.compat import build_quote_result, coerce_score
from individualStockReview.core.io import read_excel_records, safe_parse_value
from individualStockReview.core.logging import get_logger, summarize_data
from individualStockReview.utils.base import retryClass

LOGGER = get_logger("pipelines.quote_test_eval")
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_PATH = str(PROJECT_ROOT / "inputs" / "研报问答.xlsx")
DEFAULT_OUTPUT_PATH = str(PROJECT_ROOT / "outputs" / "quote_test.json")
DEFAULT_SHEET_NAME = "普通模式"
DEFAULT_GET_ANSWER_AGENT_KEY = "getAnswerCustom"
DEFAULT_QUOTE_TEST_AGENT_KEY = "quoteTest"
DEFAULT_PARALLEL_NUM = 3
DEFAULT_INNER_PARALLEL_NUM = 3
DEFAULT_RETRY_NUM = 3
DEFAULT_RETRY_INTERVAL = 2.0
DEFAULT_REQUEST_DELAY = 3.0
DEFAULT_ANSWER_REFRESH_SESSION = False
DEFAULT_QUOTE_REFRESH_SESSION = True
BOOL_STR_CHOICES = ("True", "False")


RESULT_COLUMNS = [
    "idx",
    "query",
    "result",
    "score",
    "isSucess",
    "errorInfo",
]
INNER_RESULT_COLUMNS = [
    "idx",
    "query",
    "text",
    "quote",
    "quoteContent",
    "score",
    "reason",
    "isSucess",
    "errorInfo",
]


class QuoteTestReader:
    @classmethod
    def read_records(
        cls,
        data_path: str | Path,
        *,
        sheet_name: str = DEFAULT_SHEET_NAME,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        rows = read_excel_records(data_path, sheet_name=sheet_name)
        records: list[dict[str, Any]] = []
        for idx, row in enumerate(rows):
            query = str(row.get("query") or "").strip()
            records.append(
                {
                    "idx": row.get("idx", idx),
                    "query": query,
                }
            )
            if limit is not None and len(records) >= limit:
                break
        return records


class QuoteTestEvalPipeline:
    def __init__(self, api_file: str | None = None, runtime_file: str | None = None):
        self.registry = AgentRegistry(api_file=api_file, runtime_file=runtime_file)
        self.logger = LOGGER

    @staticmethod
    def _is_missing_value(value: Any) -> bool:
        if value in (None, ""):
            return True
        try:
            return bool(pd.isna(value))
        except (TypeError, ValueError):
            return False

    @classmethod
    def _record_idx(cls, record: dict[str, Any], idx: int) -> Any:
        record_idx = record.get("idx")
        return idx if cls._is_missing_value(record_idx) else record_idx

    @classmethod
    def _record_key(cls, record: dict[str, Any], idx: int) -> str:
        return str(cls._record_idx(record, idx))

    @classmethod
    def _build_main_result_row(
        cls,
        record: dict[str, Any],
        idx: int,
        *,
        answer_payload: dict[str, Any] | None = None,
        score: Any = "",
        is_success: bool = False,
        error_info: str = "",
    ) -> dict[str, Any]:
        payload = build_quote_result()
        payload.update(
            {
                "idx": cls._record_idx(record, idx),
                "query": record.get("query") or "",
                "result": "",
                "score": score,
                "isSucess": is_success,
                "errorInfo": error_info,
            }
        )
        if answer_payload:
            payload.update(
                {
                    "result": answer_payload.get("result") or "",
                    "isSucess": bool(answer_payload.get("isSucess", is_success)),
                    "errorInfo": str(answer_payload.get("errorInfo") or error_info),
                }
            )
        payload["score"] = score
        payload["isSucess"] = is_success
        payload["errorInfo"] = error_info or payload.get("errorInfo", "")
        return {column: payload.get(column, "") for column in RESULT_COLUMNS}

    @classmethod
    def _build_inner_result_row(
        cls,
        record: dict[str, Any],
        idx: int,
        *,
        quote_payload: dict[str, Any] | None = None,
        quote: Any = "",
        text: str = "",
        quote_content: str = "",
        score: Any = "",
        reason: str = "",
        is_success: bool = False,
        error_info: str = "",
    ) -> dict[str, Any]:
        payload = {
            "idx": cls._record_idx(record, idx),
            "query": record.get("query") or "",
            "text": text,
            "quote": quote,
            "quoteContent": quote_content,
            "score": score,
            "reason": reason,
            "isSucess": is_success,
            "errorInfo": error_info,
        }
        if quote_payload:
            payload.update(
                {
                    "text": quote_payload.get("text") or text,
                    "quote": quote_payload.get("quote", quote),
                    "quoteContent": quote_payload.get("quoteContent") or quote_content,
                    "score": quote_payload.get("score", score),
                    "reason": quote_payload.get("reason") or reason,
                    "isSucess": bool(quote_payload.get("isSucess", is_success)),
                    "errorInfo": str(quote_payload.get("errorInfo") or error_info),
                }
            )
        return {column: payload.get(column, "") for column in INNER_RESULT_COLUMNS}

    @staticmethod
    def _rows_in_order(result_map: dict[str, dict[str, Any]], ordered_keys: list[str]) -> list[dict[str, Any]]:
        return [result_map[key] for key in ordered_keys if key in result_map]

    @staticmethod
    def _inner_rows_in_order(inner_result_map: dict[str, list[dict[str, Any]]], ordered_keys: list[str]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for key in ordered_keys:
            rows.extend(inner_result_map.get(key, []))
        return rows

    @staticmethod
    def _atomic_write_json(output_path: Path, payload: Any) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = output_path.with_suffix(f"{output_path.suffix}.tmp")
        with temp_path.open("w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False, indent=2)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temp_path, output_path)

    @staticmethod
    def _export_xlsx(output_path: Path, rows: list[dict[str, Any]], columns: list[str], sheet_name: str) -> Path:
        excel_path = output_path.with_suffix(".xlsx")
        excel_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows, columns=columns).to_excel(excel_path, sheet_name=sheet_name, index=False)
        return excel_path

    @staticmethod
    def _default_middle_path(output_path: Path) -> Path:
        return output_path.with_name(f"{output_path.stem}.middle.json")

    @staticmethod
    def _normalize_loaded_result(item: dict[str, Any], columns: list[str]) -> dict[str, Any]:
        return {column: item.get(column, "") for column in columns}

    @classmethod
    def _load_middle_state(cls, middle_path: Path) -> dict[str, dict[str, Any]]:
        if not middle_path.exists():
            return {}
        try:
            with middle_path.open("r", encoding="utf-8") as file:
                payload = json.load(file)
        except Exception as exc:
            LOGGER.warning("读取中间结果失败，将忽略历史 checkpoint: %s", exc)
            return {}

        items = payload.get("items", []) if isinstance(payload, dict) else []
        if not isinstance(items, list):
            return {}

        state: dict[str, dict[str, Any]] = {}
        for idx, item in enumerate(items):
            if not isinstance(item, dict):
                continue
            record = item.get("record") if isinstance(item.get("record"), dict) else {}
            raw_idx = item.get("idx")
            if cls._is_missing_value(raw_idx):
                raw_idx = record.get("idx")
            key = str(idx if cls._is_missing_value(raw_idx) else raw_idx)
            result = item.get("result") if isinstance(item.get("result"), dict) else None
            inner_results = item.get("inner_results") if isinstance(item.get("inner_results"), list) else []
            answer_payload = item.get("answer_payload") if isinstance(item.get("answer_payload"), dict) else None
            state[key] = {
                "record": record,
                "stage": str(item.get("stage") or "pending"),
                "result": cls._normalize_loaded_result(result, RESULT_COLUMNS) if isinstance(result, dict) else None,
                "inner_results": [cls._normalize_loaded_result(row, INNER_RESULT_COLUMNS) for row in inner_results if isinstance(row, dict)],
                "answer_payload": deepcopy(answer_payload) if isinstance(answer_payload, dict) else None,
                "updated_at": item.get("updated_at") or "",
            }
        return state

    @classmethod
    def _checkpoint_rows(cls, checkpoint_map: dict[str, dict[str, Any]], ordered_keys: list[str]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for key in ordered_keys:
            entry = checkpoint_map.get(key)
            if not entry:
                continue
            record = entry.get("record") or {}
            rows.append(
                {
                    "idx": record.get("idx", key),
                    "record": record,
                    "stage": entry.get("stage") or "pending",
                    "result": entry.get("result") or {},
                    "inner_results": entry.get("inner_results") or [],
                    "answer_payload": entry.get("answer_payload") or {},
                    "updated_at": entry.get("updated_at") or "",
                }
            )
        return rows

    @classmethod
    def _checkpoint_meta(
        cls,
        *,
        data_path: str,
        output_path: Path,
        middle_path: Path,
        inner_output_path: Path,
        sheet_name: str,
        checkpoint_map: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        stage_counter = {"answered": 0, "completed": 0, "failed": 0, "pending": 0}
        for entry in checkpoint_map.values():
            stage = str(entry.get("stage") or "pending")
            stage_counter[stage] = stage_counter.get(stage, 0) + 1
        return {
            "data_path": data_path,
            "output_path": str(output_path),
            "middle_path": str(middle_path),
            "inner_output_path": str(inner_output_path),
            "sheet_name": sheet_name,
            "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "stats": stage_counter,
        }

    @classmethod
    def _build_checkpoint_entry(
        cls,
        record: dict[str, Any],
        idx: int,
        *,
        stage: str,
        answer_payload: dict[str, Any] | None = None,
        result: dict[str, Any] | None = None,
        inner_results: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        return {
            "record": {
                "idx": cls._record_idx(record, idx),
                "query": record.get("query") or "",
            },
            "stage": stage,
            "answer_payload": deepcopy(answer_payload) if answer_payload else None,
            "result": deepcopy(result) if result else None,
            "inner_results": deepcopy(inner_results) if inner_results else [],
            "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }

    @staticmethod
    def _should_run(existing: dict[str, Any] | None, resume_type: str) -> bool:
        if existing is None:
            return resume_type in {"all", "unfinished"}
        stage = str(existing.get("stage") or "pending")
        if stage == "completed":
            return False
        if stage == "failed":
            return resume_type in {"all", "errors"}
        return resume_type in {"all", "unfinished"}

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

    @staticmethod
    def _safe_numeric_score(value: Any) -> float | None:
        score = coerce_score(value)
        if isinstance(score, (int, float)):
            return float(score)
        return None

    @staticmethod
    def _parse_answer_structures(answer_payload: dict[str, Any]) -> tuple[list[Any], list[Any]]:
        split_result = safe_parse_value(answer_payload.get("splitResult"), default=[])
        take_num_list = safe_parse_value(answer_payload.get("takeNumList"), default=[])
        split_result = split_result if isinstance(split_result, list) else []
        take_num_list = take_num_list if isinstance(take_num_list, list) else []
        return split_result, take_num_list

    @classmethod
    def _prepare_quote_jobs(
        cls,
        record: dict[str, Any],
        idx: int,
        answer_payload: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        split_result, take_num_list = cls._parse_answer_structures(answer_payload)
        pending_jobs: list[dict[str, Any]] = []
        prepared_results: list[dict[str, Any]] = []

        for item in split_result:
            if not isinstance(item, (list, tuple)) or len(item) < 2:
                LOGGER.warning("splitResult 结构异常，已跳过: %s", summarize_data(item))
                continue
            text = str(item[0] or "")
            quote_idx_list = item[1] if isinstance(item[1], list) else []
            for quote_idx in quote_idx_list:
                try:
                    quote_number = int(quote_idx) - 1
                    quote_item = take_num_list[quote_number]
                    quote_content = quote_item[1] if isinstance(quote_item, (list, tuple)) and len(quote_item) > 1 else ""
                except Exception as exc:
                    prepared_results.append(
                        cls._build_inner_result_row(
                            record,
                            idx,
                            quote=quote_idx,
                            text=text,
                            quote_content="",
                            is_success=False,
                            error_info=f"引用内容解析失败: {exc}",
                        )
                    )
                    continue
                pending_jobs.append(
                    {
                        "idx": cls._record_idx(record, idx),
                        "messages": record.get("query") or "",
                        "text": text,
                        "quote": quote_idx,
                        "quoteContent": quote_content,
                        "score": "",
                        "reason": "",
                        "isSucess": False,
                        "errorInfo": "",
                    }
                )
        return pending_jobs, prepared_results

    async def _get_answer_once(
        self,
        answer_agent,
        record: dict[str, Any],
        idx: int,
        request_delay: float,
        delay_lock: asyncio.Lock,
        request_state: dict[str, float],
        answer_refresh_session: bool,
    ) -> dict[str, Any]:
        await self._wait_for_request_slot(request_delay, delay_lock, request_state)
        default_result = build_quote_result()
        default_result.update({"idx": self._record_idx(record, idx), "messages": record.get("query") or ""})
        response = await answer_agent.getChatResult(
            defaultResult=default_result,
            sessionName=f"quoteTest-{self._record_key(record, idx)}",
            agentUpdateParams={"messages": record.get("query") or ""},
            replaceTrace=True,
            notTranJson=True,
            showQuoteType=True,
            getQuote=True,
            timeStatc=True,
            refreshSession=answer_refresh_session,
            retryNum=1,
            retryInterval=0.0,
        )
        if not isinstance(response, dict) or not response.get("isSucess"):
            error_info = "问答生成失败"
            if isinstance(response, dict):
                error_info = str(response.get("errorInfo") or error_info)
            raise RuntimeError(error_info)
        return response

    async def _get_answer_with_retry(
        self,
        answer_agent,
        record: dict[str, Any],
        idx: int,
        request_delay: float,
        delay_lock: asyncio.Lock,
        request_state: dict[str, float],
        retry_num: int,
        retry_interval: float,
        answer_refresh_session: bool,
    ) -> tuple[bool, Any]:
        return await retryClass.decorator(
            self._get_answer_once,
            None,
            False,
            retry_num,
            retry_interval,
            answer_agent,
            record,
            idx,
            request_delay,
            delay_lock,
            request_state,
            answer_refresh_session,
        )

    async def _score_single_quote_once(
        self,
        quote_agent,
        inner_payload: dict[str, Any],
        request_delay: float,
        delay_lock: asyncio.Lock,
        request_state: dict[str, float],
        quote_refresh_session: bool,
    ) -> dict[str, Any]:
        await self._wait_for_request_slot(request_delay, delay_lock, request_state)
        update_params = {
            "messages": "严格按照评分规则进行评分",
            "dynamicPrompt": {
                "query": inner_payload.get("messages") or "",
                "result": inner_payload.get("text") or "",
                "content": inner_payload.get("quoteContent") or "",
            },
        }
        response = await quote_agent.getChatResult(
            defaultResult=deepcopy(inner_payload),
            agentUpdateParams=update_params,
            replaceTrace=True,
            refreshSession=quote_refresh_session,
            retryNum=1,
            retryInterval=0.0,
        )
        if not isinstance(response, dict) or not response.get("isSucess"):
            error_info = "引用评分失败"
            if isinstance(response, dict):
                error_info = str(response.get("errorInfo") or error_info)
            raise RuntimeError(error_info)
        return response

    async def _score_single_quote_with_retry(
        self,
        quote_agent,
        record: dict[str, Any],
        idx: int,
        inner_payload: dict[str, Any],
        request_delay: float,
        delay_lock: asyncio.Lock,
        request_state: dict[str, float],
        retry_num: int,
        retry_interval: float,
        quote_refresh_session: bool,
    ) -> dict[str, Any]:
        retry_result = await retryClass.decorator(
            self._score_single_quote_once,
            None,
            False,
            retry_num,
            retry_interval,
            quote_agent,
            inner_payload,
            request_delay,
            delay_lock,
            request_state,
            quote_refresh_session,
        )
        if retry_result[0]:
            return self._build_inner_result_row(record, idx, quote_payload=retry_result[1], is_success=True)
        return self._build_inner_result_row(
            record,
            idx,
            quote=inner_payload.get("quote"),
            text=str(inner_payload.get("text") or ""),
            quote_content=str(inner_payload.get("quoteContent") or ""),
            is_success=False,
            error_info=str(retry_result[1]),
        )

    async def _evaluate_quote_bundle(
        self,
        quote_agent,
        record: dict[str, Any],
        idx: int,
        answer_payload: dict[str, Any],
        *,
        inner_parallel_num: int,
        request_delay: float,
        delay_lock: asyncio.Lock,
        request_state: dict[str, float],
        retry_num: int,
        retry_interval: float,
        quote_refresh_session: bool,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        pending_jobs, prepared_results = self._prepare_quote_jobs(record, idx, answer_payload)
        semaphore = asyncio.Semaphore(max(int(inner_parallel_num), 1))

        async def run_job(inner_payload: dict[str, Any]) -> dict[str, Any]:
            async with semaphore:
                return await self._score_single_quote_with_retry(
                    quote_agent,
                    record,
                    idx,
                    inner_payload,
                    request_delay,
                    delay_lock,
                    request_state,
                    retry_num,
                    retry_interval,
                    quote_refresh_session,
                )

        evaluated_results = await asyncio.gather(*(run_job(job) for job in pending_jobs)) if pending_jobs else []
        inner_results = prepared_results + list(evaluated_results)
        numeric_scores = [
            score
            for item in inner_results
            for score in [self._safe_numeric_score(item.get("score"))]
            if score is not None and bool(item.get("isSucess"))
        ]

        success_quote_count = sum(1 for item in inner_results if item.get("isSucess"))
        failed_quote_count = len(inner_results) - success_quote_count
        score = round(sum(numeric_scores) / len(numeric_scores), 4) if numeric_scores else 0.0
        error_info = str(answer_payload.get("errorInfo") or "")
        if failed_quote_count > 0 and not error_info:
            error_info = f"部分引用评分失败: {failed_quote_count}/{len(inner_results)}"
        main_result = self._build_main_result_row(
            record,
            idx,
            answer_payload=answer_payload,
            score=score,
            is_success=bool(answer_payload.get("isSucess")),
            error_info=error_info,
        )
        return main_result, inner_results

    async def evaluate_async(
        self,
        data_path: str = DEFAULT_DATA_PATH,
        save_path: str | None = None,
        middle_path: str | None = None,
        limit: int | None = None,
        run_mode: str = "resume",
        resume_type: str = "all",
        parallel_num: int = DEFAULT_PARALLEL_NUM,
        inner_parallel_num: int = DEFAULT_INNER_PARALLEL_NUM,
        request_delay: float = DEFAULT_REQUEST_DELAY,
        retry_num: int = DEFAULT_RETRY_NUM,
        retry_interval: float = DEFAULT_RETRY_INTERVAL,
        sheet_name: str = DEFAULT_SHEET_NAME,
        answer_refresh_session: bool = DEFAULT_ANSWER_REFRESH_SESSION,
        quote_refresh_session: bool = DEFAULT_QUOTE_REFRESH_SESSION,
    ) -> dict[str, Any]:
        if not str(data_path or "").strip():
            raise ValueError("data_path 不能为空，请传入输入 Excel 文件路径")
        output_path = Path(save_path or DEFAULT_OUTPUT_PATH)
        middle_result_path = Path(middle_path) if middle_path else self._default_middle_path(output_path)
        inner_output_path = output_path.with_name(f"{output_path.stem}_inner.json")
        records = QuoteTestReader.read_records(data_path=data_path, sheet_name=sheet_name, limit=limit)
        ordered_keys = [self._record_key(record, idx) for idx, record in enumerate(records)]
        checkpoint_map = {} if run_mode == "overwrite" else self._load_middle_state(middle_result_path)
        result_map = {
            key: deepcopy(entry["result"])
            for key, entry in checkpoint_map.items()
            if entry.get("stage") in {"completed", "failed"} and isinstance(entry.get("result"), dict)
        }
        inner_result_map = {
            key: deepcopy(entry.get("inner_results") or [])
            for key, entry in checkpoint_map.items()
            if entry.get("stage") == "completed"
        }

        self.logger.info(
            "quoteTest 开始执行: %s",
            summarize_data(
                {
                    "data_path": data_path,
                    "save_path": str(output_path),
                    "middle_path": str(middle_result_path),
                    "sheet_name": sheet_name,
                    "limit": limit,
                    "parallel_num": parallel_num,
                    "inner_parallel_num": inner_parallel_num,
                    "request_delay": request_delay,
                    "retry_num": retry_num,
                    "retry_interval": retry_interval,
                    "run_mode": run_mode,
                    "resume_type": resume_type,
                    "answer_refresh_session": answer_refresh_session,
                    "quote_refresh_session": quote_refresh_session,
                }
            ),
        )
        self.logger.info("quoteTest 读取到 %s 条输入记录", len(records))
        if checkpoint_map:
            stage_summary: dict[str, int] = {}
            for entry in checkpoint_map.values():
                stage = str(entry.get("stage") or "pending")
                stage_summary[stage] = stage_summary.get(stage, 0) + 1
            self.logger.info("命中历史 checkpoint: %s", summarize_data(stage_summary))

        answer_agent = self.registry.get_gf_agent(DEFAULT_GET_ANSWER_AGENT_KEY)
        quote_agent = self.registry.get_gf_agent(DEFAULT_QUOTE_TEST_AGENT_KEY)
        semaphore = asyncio.Semaphore(max(int(parallel_num), 1))
        save_lock = asyncio.Lock()
        progress_lock = asyncio.Lock()
        delay_lock = asyncio.Lock()
        request_state = {"last_started": 0.0}
        progress = {"done": 0, "success": 0, "failed": 0, "skipped": 0}
        progress_bar = tqdm(total=len(records), desc="quoteTest评测", unit="条", dynamic_ncols=True)

        async def persist() -> None:
            async with save_lock:
                rows = self._rows_in_order(result_map, ordered_keys)
                inner_rows = self._inner_rows_in_order(inner_result_map, ordered_keys)
                checkpoint_payload = {
                    "meta": self._checkpoint_meta(
                        data_path=data_path,
                        output_path=output_path,
                        middle_path=middle_result_path,
                        inner_output_path=inner_output_path,
                        sheet_name=sheet_name,
                        checkpoint_map=checkpoint_map,
                    ),
                    "items": self._checkpoint_rows(checkpoint_map, ordered_keys),
                }
                self._atomic_write_json(middle_result_path, checkpoint_payload)
                self._atomic_write_json(output_path, rows)
                self._atomic_write_json(inner_output_path, inner_rows)

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

        await persist()

        async def worker(idx: int, record: dict[str, Any]) -> None:
            key = self._record_key(record, idx)
            existing = checkpoint_map.get(key)
            if run_mode != "overwrite" and not self._should_run(existing, resume_type):
                await advance_progress("skipped")
                return

            async with semaphore:
                try:
                    answer_payload: dict[str, Any]
                    if existing and existing.get("stage") == "answered" and isinstance(existing.get("answer_payload"), dict):
                        answer_payload = deepcopy(existing["answer_payload"])
                        self.logger.info("续跑命中中间结果，直接进入引用评分: %s", key)
                    else:
                        answer_result = await self._get_answer_with_retry(
                            answer_agent,
                            record,
                            idx,
                            request_delay,
                            delay_lock,
                            request_state,
                            retry_num,
                            retry_interval,
                            answer_refresh_session,
                        )
                        if not answer_result[0]:
                            failed_result = self._build_main_result_row(
                                record,
                                idx,
                                is_success=False,
                                error_info=str(answer_result[1]),
                            )
                            checkpoint_map[key] = self._build_checkpoint_entry(
                                record,
                                idx,
                                stage="failed",
                                result=failed_result,
                            )
                            result_map[key] = failed_result
                            inner_result_map[key] = []
                            await persist()
                            self.logger.warning("问答生成失败: %s", summarize_data({"idx": key, "error": answer_result[1]}))
                            await advance_progress("failed")
                            return
                        answer_payload = deepcopy(answer_result[1])
                        checkpoint_map[key] = self._build_checkpoint_entry(
                            record,
                            idx,
                            stage="answered",
                            answer_payload=answer_payload,
                        )
                        await persist()
                        self.logger.debug("已保存回答中间结果: %s", summarize_data({"idx": key, "stage": "answered"}))

                    main_result, inner_results = await self._evaluate_quote_bundle(
                        quote_agent,
                        record,
                        idx,
                        answer_payload,
                        inner_parallel_num=inner_parallel_num,
                        request_delay=request_delay,
                        delay_lock=delay_lock,
                        request_state=request_state,
                        retry_num=retry_num,
                        retry_interval=retry_interval,
                        quote_refresh_session=quote_refresh_session,
                    )
                    checkpoint_map[key] = self._build_checkpoint_entry(
                        record,
                        idx,
                        stage="completed" if main_result.get("isSucess") else "failed",
                        answer_payload=answer_payload,
                        result=main_result,
                        inner_results=inner_results,
                    )
                    result_map[key] = main_result
                    inner_result_map[key] = inner_results
                    await persist()
                    await advance_progress("success" if main_result.get("isSucess") else "failed")
                except Exception as exc:
                    failed_result = self._build_main_result_row(record, idx, is_success=False, error_info=str(exc))
                    checkpoint_map[key] = self._build_checkpoint_entry(record, idx, stage="failed", result=failed_result)
                    result_map[key] = failed_result
                    inner_result_map[key] = []
                    await persist()
                    self.logger.exception("quoteTest 处理失败: %s", summarize_data({"idx": key, "error": str(exc)}))
                    await advance_progress("failed")

        try:
            tasks = [worker(idx, record) for idx, record in enumerate(records)]
            if tasks:
                await asyncio.gather(*tasks)
            await persist()
        finally:
            progress_bar.close()

        rows = self._rows_in_order(result_map, ordered_keys)
        inner_rows = self._inner_rows_in_order(inner_result_map, ordered_keys)
        excel_path = self._export_xlsx(output_path, rows, RESULT_COLUMNS, "results")
        inner_excel_path = self._export_xlsx(inner_output_path, inner_rows, INNER_RESULT_COLUMNS, "inner_results")
        self.logger.info(
            "quoteTest 执行完成: %s",
            summarize_data(
                {
                    "json_path": str(output_path),
                    "middle_path": str(middle_result_path),
                    "inner_json_path": str(inner_output_path),
                    "xlsx_path": str(excel_path),
                    "inner_xlsx_path": str(inner_excel_path),
                    "result_count": len(rows),
                    "inner_result_count": len(inner_rows),
                    "success": progress["success"],
                    "failed": progress["failed"],
                    "skipped": progress["skipped"],
                }
            ),
        )
        return {
            "json_path": str(output_path),
            "middle_json_path": str(middle_result_path),
            "inner_json_path": str(inner_output_path),
            "xlsx_path": str(excel_path),
            "inner_xlsx_path": str(inner_excel_path),
            "results": rows,
            "inner_results": inner_rows,
        }

    def evaluate(self, *args, **kwargs) -> dict[str, Any]:
        return asyncio.run(self.evaluate_async(*args, **kwargs))


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="研报问答引用评测脚本")
    parser.add_argument("--data-path", default=DEFAULT_DATA_PATH, help="输入 Excel 文件路径")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_PATH, help="主结果 JSON 路径，同时导出同名 xlsx")
    parser.add_argument("--middle-output", default=None, help="中间 checkpoint JSON 路径，默认与 output 同目录")
    parser.add_argument("--sheet-name", default=DEFAULT_SHEET_NAME, help="输入 Excel 的 sheet 名称")
    parser.add_argument("--use-count", type=int, default=None, help="只处理前 N 条数据")
    parser.add_argument("--parallel-num", type=int, default=DEFAULT_PARALLEL_NUM, help="并发处理的 query 数")
    parser.add_argument("--inner-parallel-num", type=int, default=DEFAULT_INNER_PARALLEL_NUM, help="单条 query 内部引用评分并发数")
    parser.add_argument("--request-delay", type=float, default=DEFAULT_REQUEST_DELAY, help="不同请求启动之间的最小间隔秒数")
    parser.add_argument("--retry-num", type=int, default=DEFAULT_RETRY_NUM, help="失败重试次数")
    parser.add_argument("--retry-interval", type=float, default=DEFAULT_RETRY_INTERVAL, help="重试间隔秒数")
    parser.add_argument(
        "--run-mode",
        choices=["resume", "overwrite"],
        default="resume",
        help="resume=基于 checkpoint 续跑，overwrite=忽略历史结果重跑",
    )
    parser.add_argument(
        "--resume-type",
        choices=["all", "unfinished", "errors"],
        default="all",
        help="all=补跑未完成和失败；unfinished=只补跑未完成；errors=只重跑失败",
    )
    parser.add_argument(
        "--answer-refresh-session",
        choices=BOOL_STR_CHOICES,
        default=str(DEFAULT_ANSWER_REFRESH_SESSION),
        metavar="True|False",
        help="第一阶段获取回答时是否刷新 sessionId，传 True 或 False",
    )
    parser.add_argument(
        "--quote-refresh-session",
        choices=BOOL_STR_CHOICES,
        default=str(DEFAULT_QUOTE_REFRESH_SESSION),
        metavar="True|False",
        help="第二阶段引用评分时是否刷新 sessionId，传 True 或 False",
    )
    parser.add_argument("--api-file", default=None, help="apiInfo.yaml 路径")
    parser.add_argument("--runtime-file", default=None, help="runtime.yaml 路径")
    return parser


def main(args: argparse.Namespace | None = None) -> dict[str, Any]:
    arguments = args or build_argument_parser().parse_args()
    pipeline = QuoteTestEvalPipeline(api_file=arguments.api_file, runtime_file=arguments.runtime_file)
    return pipeline.evaluate(
        data_path=arguments.data_path,
        save_path=arguments.output,
        middle_path=arguments.middle_output,
        limit=arguments.use_count,
        run_mode=arguments.run_mode,
        resume_type=arguments.resume_type,
        parallel_num=arguments.parallel_num,
        inner_parallel_num=arguments.inner_parallel_num,
        request_delay=arguments.request_delay,
        retry_num=arguments.retry_num,
        retry_interval=arguments.retry_interval,
        sheet_name=arguments.sheet_name,
        answer_refresh_session=arguments.answer_refresh_session == "True",
        quote_refresh_session=arguments.quote_refresh_session == "True",
    )


if __name__ == "__main__":
    main()
