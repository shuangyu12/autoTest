from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import re
import time
from pathlib import Path
from typing import Any

import pandas as pd
from tqdm import tqdm

from individualStockReview.agent.registry import AgentRegistry
from individualStockReview.core.io import safe_parse_value
from individualStockReview.core.logging import get_logger
from individualStockReview.utils.base import retryClass

LOGGER = get_logger("pipelines.stock_graph_eval")
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_AGENT_KEY = "stockGraph"
DEFAULT_SQL_PATH = ""
DEFAULT_OUTPUT_PATH = str(PROJECT_ROOT / "outputs" / "stock_graph_eval.json")
DEFAULT_AGENT_MESSAGE = (
    "请基于个股研究框架与输入图谱数据进行评测，只输出一个 JSON 对象，"
    "字段仅包含 score 和 reason。"
)
DEFAULT_PARALLEL_NUM = 5
DEFAULT_RETRY_NUM = 3
DEFAULT_REQUEST_DELAY = 3.0
RESULT_COLUMNS = [
    "id",
    "security_code",
    "framework_json",
    "graph_json",
    "score",
    "reason",
    "isSucess",
    "errorInfo",
]
SQL_COLUMNS = [
    "id",
    "research_id",
    "research_update_time",
    "security_code",
    "security_name",
    "framework_json",
    "graph_json",
    "sync_status",
    "create_time",
    "update_time",
]


class StockGraphSqlReader:
    SAMPLE_PLACEHOLDER = "{{sample}}"

    @classmethod
    def read_records(cls, data_path: str | Path, limit: int | None = None) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        with Path(data_path).open("r", encoding="utf-8") as file:
            for line_no, raw_line in enumerate(file, start=1):
                line = raw_line.strip()
                if not line.startswith("INSERT INTO `stock_graph` VALUES"):
                    continue
                for value_group in cls._extract_value_groups(line, line_no):
                    row = cls._build_row(value_group, line_no)
                    records.append(
                        {
                            "id": row.get("id"),
                            "security_code": row.get("security_code") or "",
                            "framework_json": cls._sanitize_json_text(row.get("framework_json")),
                            "graph_json": cls._sanitize_json_text(row.get("graph_json")),
                        }
                    )
                    if limit is not None and len(records) >= limit:
                        return records
        return records

    @classmethod
    def _sanitize_json_text(cls, value: Any) -> str:
        if value in (None, ""):
            return ""
        parsed = safe_parse_value(value, default=None)
        if isinstance(parsed, (dict, list)):
            sanitized = cls._replace_sample_fields(parsed)
            return json.dumps(sanitized, ensure_ascii=False, separators=(",", ":"))
        return str(value)

    @classmethod
    def _replace_sample_fields(cls, data: Any) -> Any:
        if isinstance(data, dict):
            return {
                key: cls.SAMPLE_PLACEHOLDER if key == "sample" else cls._replace_sample_fields(val)
                for key, val in data.items()
            }
        if isinstance(data, list):
            return [cls._replace_sample_fields(item) for item in data]
        return data

    @staticmethod
    def _extract_value_groups(line: str, line_no: int) -> list[str]:
        try:
            values_text = line.split("VALUES", 1)[1].strip().rstrip(";")
        except IndexError as exc:
            raise ValueError(f"第 {line_no} 行不是有效的 INSERT 语句") from exc

        groups: list[str] = []
        current: list[str] = []
        depth = 0
        in_string = False
        escape = False

        for char in values_text:
            if depth > 0:
                current.append(char)

            if escape:
                escape = False
                continue
            if in_string and char == "\\":
                escape = True
                continue
            if char == "'":
                in_string = not in_string
                continue
            if in_string:
                continue

            if char == "(":
                if depth == 0:
                    current = []
                depth += 1
                if depth > 1:
                    current.append(char)
                continue

            if char == ")":
                depth -= 1
                if depth < 0:
                    raise ValueError(f"第 {line_no} 行括号不匹配")
                if depth == 0:
                    groups.append("".join(current[:-1]))
                    current = []

        if depth != 0:
            raise ValueError(f"第 {line_no} 行括号未闭合")
        return groups

    @classmethod
    def _build_row(cls, values_group: str, line_no: int) -> dict[str, Any]:
        values = cls._parse_values_group(values_group)
        if len(values) != len(SQL_COLUMNS):
            raise ValueError(
                f"第 {line_no} 行字段数不匹配，期望 {len(SQL_COLUMNS)} 个，实际 {len(values)} 个"
            )
        return dict(zip(SQL_COLUMNS, values))

    @staticmethod
    def _parse_values_group(values_group: str) -> list[Any]:
        reader = csv.reader(
            [values_group],
            delimiter=",",
            quotechar="'",
            escapechar="\\",
            doublequote=False,
            skipinitialspace=True,
        )
        return [StockGraphSqlReader._coerce_sql_value(item) for item in next(reader)]

    @staticmethod
    def _coerce_sql_value(value: Any) -> Any:
        text = str(value).strip()
        if not text:
            return ""
        if text.upper() == "NULL":
            return None
        if re.fullmatch(r"-?\d+", text):
            return int(text)
        if re.fullmatch(r"-?\d+\.\d+", text):
            return float(text)
        return text


class StockGraphEvalPipeline:
    def __init__(self, api_file: str | None = None, runtime_file: str | None = None):
        self.registry = AgentRegistry(api_file=api_file, runtime_file=runtime_file)
        self.logger = LOGGER

    @staticmethod
    def _record_key(record: dict[str, Any], idx: int) -> str:
        record_id = record.get("id")
        return str(record_id) if record_id not in (None, "") else f"row-{idx}"

    @classmethod
    def _build_result_row(
        cls,
        record: dict[str, Any],
        idx: int,
        *,
        score: Any = "",
        reason: Any = "",
        is_success: bool = False,
        error_info: str = "",
    ) -> dict[str, Any]:
        return {
            "id": record.get("id") if record.get("id") not in (None, "") else cls._record_key(record, idx),
            "security_code": record.get("security_code") or "",
            "framework_json": record.get("framework_json") or "",
            "graph_json": record.get("graph_json") or "",
            "score": score,
            "reason": reason,
            "isSucess": is_success,
            "errorInfo": error_info,
        }

    @staticmethod
    def _extract_result_dict(raw_output: Any) -> dict[str, Any]:
        if isinstance(raw_output, dict):
            parsed = raw_output
        else:
            text = str(raw_output or "").strip()
            parsed = safe_parse_value(text, default=None)
            if not isinstance(parsed, dict):
                fenced = re.search(r"```(?:json)?\s*([\s\S]*?)```", text, flags=re.IGNORECASE)
                if fenced:
                    parsed = safe_parse_value(fenced.group(1).strip(), default=None)
            if not isinstance(parsed, dict):
                start = text.find("{")
                end = text.rfind("}")
                if start != -1 and end > start:
                    parsed = safe_parse_value(text[start : end + 1], default=None)

        if not isinstance(parsed, dict):
            raise ValueError("模型输出不是合法 JSON 对象")
        if "score" not in parsed or "reason" not in parsed:
            raise ValueError("模型输出缺少 score 或 reason 字段")
        return parsed

    @classmethod
    def _parse_agent_output(cls, raw_output: Any) -> tuple[Any, Any]:
        result = cls._extract_result_dict(raw_output)
        return result.get("score", ""), result.get("reason", "")

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
            state[key] = {column: item.get(column, "") for column in RESULT_COLUMNS}
        return state

    @staticmethod
    def _rows_in_order(result_map: dict[str, dict[str, Any]], ordered_keys: list[str]) -> list[dict[str, Any]]:
        return [result_map[key] for key in ordered_keys if key in result_map]

    @staticmethod
    def _atomic_write_json(output_path: Path, rows: list[dict[str, Any]]) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = output_path.with_suffix(f"{output_path.suffix}.tmp")
        with temp_path.open("w", encoding="utf-8") as file:
            json.dump(rows, file, ensure_ascii=False, indent=2)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temp_path, output_path)

    @staticmethod
    def _export_xlsx(output_path: Path, rows: list[dict[str, Any]]) -> Path:
        excel_path = output_path.with_suffix(".xlsx")
        excel_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows, columns=RESULT_COLUMNS).to_excel(excel_path, sheet_name="results", index=False)
        return excel_path

    @staticmethod
    def _should_run(existing: dict[str, Any] | None, resume_type: str) -> bool:
        if resume_type == "unfinished":
            return existing is None
        if resume_type == "errors":
            return existing is not None and not bool(existing.get("isSucess"))
        return existing is None or not bool(existing.get("isSucess"))

    @staticmethod
    def _render_progress(done: int, total: int, success: int, failed: int, skipped: int, width: int = 30) -> str:
        ratio = 1.0 if total <= 0 else min(max(done / total, 0.0), 1.0)
        filled = int(width * ratio)
        bar = "#" * filled + "-" * (width - filled)
        return (
            f"\r进度 [{bar}] {done}/{total} ({ratio * 100:6.2f}%) "
            f"成功:{success} 失败:{failed} 跳过:{skipped}"
        )

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
        agent,
        record: dict[str, Any],
        idx: int,
        request_delay: float,
        delay_lock: asyncio.Lock,
        request_state: dict[str, float],
    ) -> tuple[Any, Any]:
        framework_json = record.get("framework_json") or ""
        graph_json = record.get("graph_json") or ""
        await self._wait_for_request_slot(request_delay, delay_lock, request_state)
        response = await agent.getChatResult(
            defaultResult=self._build_result_row(record, idx),
            agentUpdateParams={
                "messages": f"{DEFAULT_AGENT_MESSAGE}\n输入图谱数据：{graph_json}",
                "dynamicPrompt": {"个股研究框架": framework_json},
            },
            notTranJson=True,
            replaceTrace=True,
            retryNum=1,
            retryInterval=0.0,
        )
        if not isinstance(response, dict) or not response.get("isSucess"):
            error_info = "模型调用失败"
            if isinstance(response, dict):
                error_info = str(response.get("errInfo") or error_info)
            raise RuntimeError(error_info)
        return self._parse_agent_output(response.get("result", ""))

    async def _evaluate_single(
        self,
        agent,
        record: dict[str, Any],
        idx: int,
        request_delay: float,
        delay_lock: asyncio.Lock,
        request_state: dict[str, float],
    ) -> dict[str, Any]:
        retry_result = await retryClass.decorator(
            self._evaluate_once,
            None,
            False,
            DEFAULT_RETRY_NUM,
            0.0,
            agent,
            record,
            idx,
            request_delay,
            delay_lock,
            request_state,
        )
        if retry_result[0]:
            score, reason = retry_result[1]
            return self._build_result_row(record, idx, score=score, reason=reason, is_success=True)
        return self._build_result_row(record, idx, is_success=False, error_info=str(retry_result[1]))

    async def evaluate_async(
        self,
        data_path: str = DEFAULT_SQL_PATH,
        save_path: str | None = None,
        limit: int | None = None,
        run_mode: str = "resume",
        resume_type: str = "all",
        parallel_num: int = DEFAULT_PARALLEL_NUM,
        request_delay: float = DEFAULT_REQUEST_DELAY,
    ) -> dict[str, Any]:
        if not str(data_path or "").strip():
            raise ValueError("data_path 不能为空，请通过参数传入输入 SQL 文件路径")
        output_path = Path(save_path or DEFAULT_OUTPUT_PATH)
        records = StockGraphSqlReader.read_records(data_path=data_path, limit=limit)
        ordered_keys = [self._record_key(record, idx) for idx, record in enumerate(records)]
        result_map = {} if run_mode == "overwrite" else self._load_state(output_path)
        agent = self.registry.get_gf_agent(DEFAULT_AGENT_KEY)
        semaphore = asyncio.Semaphore(max(int(parallel_num), 1))
        save_lock = asyncio.Lock()
        progress_lock = asyncio.Lock()
        delay_lock = asyncio.Lock()
        request_state = {"last_started": 0.0}
        progress = {"done": 0, "success": 0, "failed": 0, "skipped": 0}
        total_records = len(records)
        progress_bar = tqdm(total=total_records, desc="stockGraph评测", unit="条", dynamic_ncols=True)

        async def persist() -> None:
            async with save_lock:
                rows = self._rows_in_order(result_map, ordered_keys)
                self._atomic_write_json(output_path, rows)

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
                result = await self._evaluate_single(agent, record, idx, request_delay, delay_lock, request_state)
            result_map[key] = result
            await persist()
            await advance_progress("success" if result.get("isSucess") else "failed")

        try:
            tasks = [worker(idx, record) for idx, record in enumerate(records)]
            if tasks:
                await asyncio.gather(*tasks)
            await persist()
        finally:
            progress_bar.close()

        rows = self._rows_in_order(result_map, ordered_keys)
        excel_path = self._export_xlsx(output_path, rows)
        self.logger.info("stockGraph 评测完成，结果条数=%s", len(rows))
        return {"json_path": str(output_path), "xlsx_path": str(excel_path), "results": rows}

    def evaluate(self, *args, **kwargs) -> dict[str, Any]:
        return asyncio.run(self.evaluate_async(*args, **kwargs))


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="基于 GF stockGraph 智能体的个股图谱评测脚本")
    parser.add_argument("--data-path", "--input-path", dest="data_path", required=True, help="输入 SQL 文件路径")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_PATH, help="中间 JSON 保存路径，最终会生成同名 xlsx")
    parser.add_argument("--use-count", "--limit", dest="use_count", type=int, default=None, help="只处理前 N 条数据")
    parser.add_argument("--parallel-num", type=int, default=DEFAULT_PARALLEL_NUM, help="并发评测数，默认 5")
    parser.add_argument("--request-delay", type=float, default=DEFAULT_REQUEST_DELAY, help="每次请求之间的最小间隔秒数，默认 3")
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
    parser.add_argument("--api-file", default=None, help="apiInfo.yaml 路径")
    parser.add_argument("--runtime-file", default=None, help="runtime.yaml 路径")
    return parser


def main(args: argparse.Namespace | None = None) -> dict[str, Any]:
    arguments = args or build_argument_parser().parse_args()
    pipeline = StockGraphEvalPipeline(api_file=arguments.api_file, runtime_file=arguments.runtime_file)
    return pipeline.evaluate(
        data_path=arguments.data_path,
        save_path=arguments.output,
        limit=arguments.use_count,
        run_mode=arguments.run_mode,
        resume_type=arguments.resume_type,
        parallel_num=arguments.parallel_num,
        request_delay=arguments.request_delay,
    )


if __name__ == "__main__":
    main()
