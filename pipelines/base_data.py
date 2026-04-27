from __future__ import annotations

import asyncio
import os
from copy import deepcopy
from pathlib import Path
from typing import Any

import aiohttp

from individualStockReview.agent.registry import AgentRegistry
from individualStockReview.core.compat import (
    build_answer_score_result,
    build_query_result,
    build_quote_result,
    build_select_query_result,
    coerce_score,
)
from individualStockReview.core.config import ConfigManager
from individualStockReview.core.io import read_excel_records, read_json_file, safe_parse_value, write_excel_records, write_json_file
from individualStockReview.core.logging import get_logger
from individualStockReview.core.paths import PathManager
from individualStockReview.metrics.summary import summarize_recall_results, summarize_simple_result
from individualStockReview.utils.base import retryClass


class BaseDataPipeline:
    def __init__(self, api_file: str | None = None, runtime_file: str | None = None):
        self.config_manager = ConfigManager(api_file=api_file, runtime_file=runtime_file)
        self.path_manager = PathManager(runtime_config=self.config_manager.runtime_args)
        self.path_manager.ensure_directories()
        self.registry = AgentRegistry(api_file=api_file, runtime_file=runtime_file)
        self.logger = get_logger("pipelines.base_data")

    @staticmethod
    async def _extract_rows(response: aiohttp.ClientResponse, *args, **kwargs) -> list[dict[str, Any]]:
        result = await response.json()
        return result["data"]["rows"]

    @staticmethod
    async def _extract_report_id(response: aiohttp.ClientResponse, *args, **kwargs) -> str:
        result = await response.json()
        return result["data"]["rows"][0]["id"]

    async def _get_report_rows(self, api_key: str) -> list[dict[str, Any]]:
        api_params = self.registry.clone_api(api_key)
        result = await retryClass.decorator(self._extract_rows, api_params)
        if not result[0]:
            self.logger.error("读取报告列表失败: %s", result[1])
            return []
        return result[1]

    async def _get_report_id_by_name(self, file_name: str) -> tuple[bool, Any]:
        api_params = self.registry.clone_api("getReportByFileName")
        api_params.setdefault("json", {}).update({"keyword": file_name})
        return await retryClass.decorator(self._extract_report_id, api_params)

    @staticmethod
    async def _bounded(semaphore: asyncio.Semaphore, func, *args, **kwargs):
        async with semaphore:
            return await func(*args, **kwargs)

    def generate_queries(self, save_path: str | None = None, is_save: bool = True, parallel_num: int = 1, limit: int = 50):
        return asyncio.run(self.generate_queries_async(save_path=save_path, is_save=is_save, parallel_num=parallel_num, limit=limit))

    async def generate_queries_async(self, save_path: str | None = None, is_save: bool = True, parallel_num: int = 1, limit: int = 50):
        result_path = Path(save_path) if save_path else self.path_manager.output_path("save_data_path.xlsx")
        agent = self.registry.get_gf_agent("queryGenerate")
        research_report_info = await self._get_report_rows("getResearchReport")
        semaphore = asyncio.Semaphore(parallel_num)
        tasks = []
        for data in research_report_info[:limit]:
            if not data.get("fileDataId"):
                continue
            default_result = build_query_result()
            default_result.update({"fileId": data.get("id"), "fileName": data.get("fileName")})
            agent_update_params = {
                "messages": "参考研报, 设计出一个全面、简洁、具有强相关性的query。",
                "recordDocs": [{"docName": default_result["fileName"] or "研报标题", "docId": default_result["fileId"]}],
            }
            tasks.append(
                self._bounded(
                    semaphore,
                    agent.getChatResult,
                    defaultResult=deepcopy(default_result),
                    agentUpdateParams=agent_update_params,
                    replaceTrace=True,
                )
            )
        total_result = await asyncio.gather(*tasks) if tasks else []
        if is_save:
            write_excel_records([data for data in total_result if data.get("isSucess")], result_path)
        return total_result

    def integrate_results(self, path_list: list[str], real_result_path: str, save_path: str | None = None):
        result_path = Path(save_path) if save_path else self.path_manager.output_path("newData.xlsx")
        result_list = [read_excel_records(path) for path in path_list]
        real_result = read_excel_records(real_result_path)
        new_result_list = []
        if not result_list:
            write_excel_records([], result_path)
            return []

        for query_idx in range(len(result_list[0])):
            new_result = {"实际召回文档": {}}
            for result_idx in range(len(result_list)):
                result = result_list[result_idx][query_idx]
                new_result.update({"query": result.get("query")})
                for file_name in safe_parse_value(result.get("实际召回文档"), default=[]):
                    new_result["实际召回文档"][file_name] = new_result["实际召回文档"].get(file_name, 0) + 1
            new_result_list.append(new_result)

        for idx, result in enumerate(real_result):
            file_name = result.get("fileName")
            if file_name not in new_result_list[idx]["实际召回文档"]:
                new_result_list[idx]["实际召回文档"][file_name] = 10086
            else:
                new_result_list[idx]["实际召回文档"][file_name] += 10086
        write_excel_records(new_result_list, result_path)
        return new_result_list

    async def _judge_candidate(self, judge_agent, default_result: dict[str, Any], file_name: str):
        record_id = await self._get_report_id_by_name(file_name)
        if not record_id[0]:
            failed = deepcopy(default_result)
            failed.update({"isSucess": False, "errorInfo": record_id[1], "realFile": file_name})
            return failed
        update_default = deepcopy(default_result)
        update_default["realFile"] = file_name
        update_default.pop("fileList", None)
        agent_update_params = {
            "messages": default_result["messages"],
            "recordDocs": [{"docName": file_name, "docId": record_id[1]}],
        }
        return await judge_agent.getChatResult(
            defaultResult=update_default,
            agentUpdateParams=agent_update_params,
            replaceTrace=True,
        )

    def select_queries(self, data_path: str, save_path: str | None = None, middle_path: str | None = None, parallel_num: int = 10, is_save: bool = True):
        return asyncio.run(
            self.select_queries_async(
                data_path=data_path,
                save_path=save_path,
                middle_path=middle_path,
                parallel_num=parallel_num,
                is_save=is_save,
            )
        )

    async def select_queries_async(self, data_path: str, save_path: str | None = None, middle_path: str | None = None, parallel_num: int = 10, is_save: bool = True):
        result_path = Path(save_path) if save_path else self.path_manager.output_path("finally_data_path.xlsx")
        middle_result_path = Path(middle_path) if middle_path else self.path_manager.artifact_path("selectQueryMiddleResult.json")
        judge_agent = self.registry.get_gf_agent("judgeAgent")
        semaphore = asyncio.Semaphore(parallel_num)
        data_list = read_excel_records(data_path)
        total_result_map: dict[int, dict[str, Any]] = {}
        tasks = []

        port_result = read_json_file(middle_result_path, default=None) if middle_result_path.exists() else None

        for idx, data in enumerate(data_list):
            default_result = build_select_query_result()
            default_result.update(
                {
                    "idx": idx,
                    "queryKeyWord": safe_parse_value(data.get("reason"), default=data.get("reason")),
                    "messages": data.get("query"),
                    "fileList": safe_parse_value(data.get("实际召回文档"), default={}) or {},
                    "realFile": [],
                    "isSucess": True,
                }
            )
            total_result_map[idx] = default_result
            if not isinstance(default_result["fileList"], dict):
                default_result["fileList"] = {}
            for file_name, file_num in default_result["fileList"].items():
                if file_num > 3:
                    if file_name not in default_result["realFile"]:
                        default_result["realFile"].append(file_name)
                elif port_result is None:
                    tasks.append(self._bounded(semaphore, self._judge_candidate, judge_agent, deepcopy(default_result), file_name))

        if port_result is None:
            port_result = await asyncio.gather(*tasks) if tasks else []
            write_json_file(middle_result_path, port_result)

        for result in port_result:
            score = coerce_score(result.get("score"))
            if not isinstance(score, (int, float)) or score < 0.75:
                continue
            idx = int(result["idx"])
            record = total_result_map[idx]
            record["score"] = score
            record["reason"] = result.get("reason", record.get("reason", ""))
            if result.get("realFile") not in record["realFile"]:
                record["realFile"].append(result["realFile"])

        total_result = [total_result_map[idx] for idx in sorted(total_result_map.keys())]
        if is_save:
            write_excel_records([data for data in total_result if data.get("isSucess")], result_path)
        return total_result

    async def _score_single_quote(self, quote_agent, inner_result: dict[str, Any]):
        quote_idx = inner_result.get("quote")
        update_params = {
            "messages": "严格按照评分规则进行评分",
            "dynamicPrompt": {
                "query": inner_result["messages"],
                "result": inner_result["text"],
                "content": inner_result["quoteContent"],
            },
        }
        payload = await quote_agent.getChatResult(defaultResult=deepcopy(inner_result), agentUpdateParams=update_params)
        payload["quote"] = quote_idx
        payload.pop("quoteContent", None)
        return payload

    async def _evaluate_quote_bundle(self, quote_agent, default_result: dict[str, Any], inner_parallel_num: int):
        tasks = []
        semaphore = asyncio.Semaphore(inner_parallel_num)
        take_num_list = default_result.get("takeNumList", [])
        split_result = default_result.get("splitResult", [])
        for result in split_result:
            text = result[0]
            quote_idx_list = result[1]
            for quote_idx in quote_idx_list:
                quote_number = int(quote_idx) - 1
                quote_content = take_num_list[quote_number][1]
                inner_result = {
                    "idx": default_result["idx"],
                    "messages": default_result["messages"],
                    "text": text,
                    "quote": quote_idx,
                    "quoteContent": quote_content,
                    "score": "",
                    "reason": "",
                }
                tasks.append(self._bounded(semaphore, self._score_single_quote, quote_agent, inner_result))

        inner_results = await asyncio.gather(*tasks) if tasks else []
        default_result["score"] = (
            sum([coerce_score(inner_result.get("score", 0)) for inner_result in inner_results]) / len(inner_results)
            if inner_results
            else 0
        )
        return default_result, inner_results

    def quote_test(self, data_path: str, save_path: str | None = None, middle_path: str | None = None, parallel_num: int = 1, inner_parallel_num: int = 6, is_save: bool = True, sheet_name: str = "研报问答-广发模式"):
        return asyncio.run(
            self.quote_test_async(
                data_path=data_path,
                save_path=save_path,
                middle_path=middle_path,
                parallel_num=parallel_num,
                inner_parallel_num=inner_parallel_num,
                is_save=is_save,
                sheet_name=sheet_name,
            )
        )

    async def quote_test_async(self, data_path: str, save_path: str | None = None, middle_path: str | None = None, parallel_num: int = 1, inner_parallel_num: int = 6, is_save: bool = True, sheet_name: str = "研报问答-广发模式"):
        result_path = Path(save_path) if save_path else self.path_manager.output_path("percision_path_restore.xlsx")
        middle_result_path = Path(middle_path) if middle_path else self.path_manager.artifact_path("middleResult.json")
        get_answer_agent = self.registry.get_gf_agent("getAnswerCustom")
        quote_test_agent = self.registry.get_gf_agent("quoteTest")
        semaphore = asyncio.Semaphore(parallel_num)

        if middle_result_path.exists():
            total_result = read_json_file(middle_result_path, default=[])
        else:
            data_list = read_excel_records(data_path, sheet_name=sheet_name)
            tasks = []
            for idx, data in enumerate(data_list):
                default_result = build_quote_result()
                default_result.update({"messages": data.get("query"), "idx": idx})
                session_name = f"问答测试2-{idx}"
                agent_update_params = {"messages": default_result["messages"]}
                tasks.append(
                    self._bounded(
                        semaphore,
                        get_answer_agent.getChatResult,
                        defaultResult=deepcopy(default_result),
                        sessionName=session_name,
                        agentUpdateParams=agent_update_params,
                        replaceTrace=True,
                        notTranJson=True,
                        showQuoteType=True,
                        getQuote=True,
                        timeStatc=True,
                    )
                )
            total_result = await asyncio.gather(*tasks) if tasks else []
            write_json_file(middle_result_path, total_result)

        tasks = []
        for data in total_result:
            tasks.append(self._evaluate_quote_bundle(quote_test_agent, deepcopy(data), inner_parallel_num=inner_parallel_num))
        evaluated_result = await asyncio.gather(*tasks) if tasks else []

        if is_save:
            final_records = [item[0] for item in evaluated_result if item[0].get("isSucess")]
            inner_records = []
            for item in evaluated_result:
                inner_records.extend(item[1])
            write_excel_records(final_records, result_path)
            inner_path = result_path.with_name(f"{result_path.stem}_inner{result_path.suffix}")
            write_excel_records(inner_records, inner_path)
        return evaluated_result

    def answer_test(self, data_path: str, save_path: str | None = None, parallel_num: int = 5, is_save: bool = True):
        return asyncio.run(self.answer_test_async(data_path=data_path, save_path=save_path, parallel_num=parallel_num, is_save=is_save))

    async def answer_test_async(self, data_path: str, save_path: str | None = None, parallel_num: int = 5, is_save: bool = True):
        result_path = Path(save_path) if save_path else self.path_manager.output_path("new_accurate_path_default.xlsx")
        get_answer_agent = self.registry.get_gf_agent("reportAnswerTest")
        semaphore = asyncio.Semaphore(parallel_num)
        data_list = read_excel_records(data_path)
        tasks = []
        for data in data_list:
            default_result = build_answer_score_result()
            default_result.update({"idx": data.get("idx"), "messages": data.get("messages"), "result": data.get("result")})
            agent_update_params = {
                "messages": "结合你的专业知识与业务了解，参考生成规则，严格按照评审规则进行问答效果的评审",
                "dynamicPrompt": {"query": default_result["messages"], "answer": default_result["result"]},
            }
            tasks.append(
                self._bounded(
                    semaphore,
                    get_answer_agent.getChatResult,
                    defaultResult=deepcopy(default_result),
                    agentUpdateParams=agent_update_params,
                    replaceTrace=True,
                )
            )
        total_result = await asyncio.gather(*tasks) if tasks else []
        if is_save:
            write_excel_records([data for data in total_result if data.get("isSucess")], result_path)
        return total_result

    def summarize_recall_results(self, path_list: list[str], real_result_path: str, save_path: str | None = None):
        return summarize_recall_results(path_list=path_list, real_result_path=real_result_path, save_path=save_path)

    def summarize_simple_result(self, result_path: str, save_back: bool = True):
        return summarize_simple_result(result_path=result_path, save_back=save_back)
