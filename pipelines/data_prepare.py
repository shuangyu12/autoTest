from __future__ import annotations

import asyncio
import datetime
import json
import random
from copy import deepcopy
from pathlib import Path
from typing import Any

import aiohttp

from individualStockReview.agent.registry import AgentRegistry
from individualStockReview.core.compat import build_data_prepare_result
from individualStockReview.core.config import ConfigManager
from individualStockReview.core.io import read_excel_records, safe_parse_value, write_excel_records
from individualStockReview.core.logging import get_logger
from individualStockReview.core.paths import PathManager
from individualStockReview.utils.base import retryClass


class DataPreparePipeline:
    def __init__(self, api_file: str | None = None, runtime_file: str | None = None):
        self.config_manager = ConfigManager(api_file=api_file, runtime_file=runtime_file)
        self.path_manager = PathManager(runtime_config=self.config_manager.runtime_args)
        self.path_manager.ensure_directories()
        self.registry = AgentRegistry(api_file=api_file, runtime_file=runtime_file)
        self.logger = get_logger("pipelines.data_prepare")

    @staticmethod
    async def _extract_rows(response: aiohttp.ClientResponse, *args, **kwargs) -> list[dict[str, Any]]:
        result = await response.json()
        return result["data"]["rows"]

    @staticmethod
    async def _extract_es_records(response: aiohttp.ClientResponse, *args, **kwargs) -> list[dict[str, Any]]:
        result = await response.json()
        return result.get("data", {}).get("recordList", [])

    async def _get_report_rows(self, api_key: str) -> list[dict[str, Any]]:
        api_params = self.registry.clone_api(api_key)
        result = await retryClass.decorator(self._extract_rows, api_params)
        return result[1] if result[0] else []

    async def _get_es_records(self, index_name: str, group_id: str, size: int = 1) -> list[dict[str, Any]]:
        api_params = self.registry.clone_api("getEsInfo")
        query_dsl = {"query": {"bool": {"must": [{"term": {"group": group_id}}]}}, "size": size}
        api_params.setdefault("json", {}).update({"indexName": index_name, "queryDsl": json.dumps(query_dsl, ensure_ascii=False)})
        result = await retryClass.decorator(self._extract_es_records, api_params)
        return result[1] if result[0] else []

    async def _collect_reports(self, api_key: str, index_name: str, include_sub_type_filter: bool = False) -> dict[str, list[dict[str, Any]]]:
        rows = await self._get_report_rows(api_key)
        result: dict[str, list[dict[str, Any]]] = {}
        stock_suffixes = [".SH", ".SZ", ".BJ"]
        report_types = ["季报点评", "中报点评", "年报点评", "公司深度研究报告"]
        now = datetime.datetime.now()
        start_date = now - datetime.timedelta(days=2 * 365)

        for data in rows:
            file_data_id = data.get("fileDataId", "")
            if not file_data_id:
                continue
            es_data_list = await self._get_es_records(index_name=index_name, group_id=file_data_id)
            if not es_data_list:
                continue
            meta = es_data_list[0].get("source_data", {}).get("metadata", {})
            if include_sub_type_filter:
                sub_type = meta.get("report_sub_type", "")
                if not any(report_type == sub_type for report_type in report_types):
                    continue

            security_code = meta.get("security_codes", "")
            if isinstance(security_code, list):
                security_code = security_code[0]
            if not security_code or not any(suffix in security_code for suffix in stock_suffixes):
                continue

            report_date_str = meta.get("archive_time", meta.get("archiveTime", meta.get("report_date", "")))
            if not report_date_str:
                continue
            try:
                report_date = datetime.datetime.strptime(report_date_str, "%Y-%m-%d")
            except ValueError:
                continue
            if not (start_date <= report_date <= now):
                continue
            if index_name == "knowledge_base_content_financial_report" and not (
                report_date_str.endswith("-06-30") or report_date_str.endswith("-12-31")
            ):
                continue

            item = {
                "security_code": security_code,
                "id": data.get("id"),
                "fileName": data.get("fileName"),
                "filePath": data.get("filePath"),
                "fileDataId": data.get("fileDataId"),
                "report_date": report_date_str,
                "stockName": meta.get("stock_names", [""])[0] if isinstance(meta.get("stock_names", ""), list) else meta.get("stock_names", ""),
                "baseId": 1 if index_name == "knowledge_base_content_report" else 4,
            }
            result.setdefault(security_code, []).append(item)
        return result

    def collect_report_pairs(self, save_path: str | None = None, include_file_paths: bool = False, require_research_report: bool = False, is_save: bool = True):
        return asyncio.run(
            self.collect_report_pairs_async(
                save_path=save_path,
                include_file_paths=include_file_paths,
                require_research_report=require_research_report,
                is_save=is_save,
            )
        )

    async def collect_report_pairs_async(self, save_path: str | None = None, include_file_paths: bool = False, require_research_report: bool = False, is_save: bool = True):
        result_path = Path(save_path) if save_path else self.path_manager.output_path("save_data_path.xlsx")
        research_report_data, financial_report_data = await asyncio.gather(
            self._collect_reports("getResearchReport", "knowledge_base_content_report", include_sub_type_filter=True),
            self._collect_reports("getFinancialReport", "knowledge_base_content_financial_report", include_sub_type_filter=False),
        )

        total_data: dict[str, dict[str, list[dict[str, Any]]]] = {}
        for security_code, data in financial_report_data.items():
            total_data.setdefault(security_code, {"financialReport": [], "researchReport": []})
            total_data[security_code]["financialReport"].extend(sorted(data, key=lambda item: item["report_date"]))
        for security_code, data in research_report_data.items():
            total_data.setdefault(security_code, {"financialReport": [], "researchReport": []})
            total_data[security_code]["researchReport"].extend(sorted(data, key=lambda item: item["report_date"]))

        final_data = []
        for security_code, data in total_data.items():
            if require_research_report and len(data["researchReport"]) == 0:
                continue
            payload = build_data_prepare_result(include_file_paths=include_file_paths)
            payload["security_code"] = security_code
            payload["stockName"] = data["researchReport"][0]["stockName"] if data["researchReport"] else data["financialReport"][0]["stockName"]
            if len(data["researchReport"]) > 1 or len(data["financialReport"]) > 1:
                first_docs = data["researchReport"][:-1] + data["financialReport"][:-1]
                second_docs = [data["researchReport"][-1]] if data["researchReport"] else []
                second_docs += [data["financialReport"][-1]] if data["financialReport"] else []
                payload["firstPortId"] = [(item["id"], item["baseId"], item["fileName"], item["report_date"]) for item in first_docs]
                payload["secondPortId"] = [(item["id"], item["baseId"], item["fileName"], item["report_date"]) for item in second_docs]
                if include_file_paths:
                    payload["firstFilePath"] = [(item["id"], item["filePath"]) for item in first_docs]
                    payload["secondFilePath"] = [(item["id"], item["filePath"]) for item in second_docs]
            else:
                first_docs = data["researchReport"] + data["financialReport"]
                payload["firstPortId"] = [(item["id"], item["baseId"], item["fileName"], item["report_date"]) for item in first_docs]
                if include_file_paths:
                    payload["firstFilePath"] = [(item["id"], item["filePath"]) for item in first_docs]
            final_data.append(payload)

        if is_save:
            write_excel_records(final_data, result_path)
        return final_data

    @staticmethod
    def _build_record_docs(port_ids: list[Any], allow_types: list[int] | None = None) -> list[dict[str, Any]]:
        record_docs = []
        for item in port_ids:
            if allow_types and item[1] not in allow_types:
                continue
            record_docs.append({"docName": item[2] or "研报标题", "docId": item[0]})
        return record_docs

    def _read_records(self, path: str) -> list[dict[str, Any]]:
        return read_excel_records(path)

    async def _run_chat_flow(self, agent_key: str, default_result: dict[str, Any], agent_update_params: dict[str, Any], replace_trace: bool = True, not_tran_json: bool = False):
        agent = self.registry.get_gf_agent(agent_key)
        return await agent.getChatResult(
            defaultResult=deepcopy(default_result),
            agentUpdateParams=deepcopy(agent_update_params),
            replaceTrace=replace_trace,
            notTranJson=not_tran_json,
        )

    def first_result(self, finally_data_path: str, save_path: str | None = None, is_save: bool = False):
        return asyncio.run(self.first_result_async(finally_data_path=finally_data_path, save_path=save_path, is_save=is_save))

    async def first_result_async(self, finally_data_path: str, save_path: str | None = None, is_save: bool = False):
        result_path = Path(save_path) if save_path else self.path_manager.output_path("save_firstResult_path.xlsx")
        final_data = self._read_records(finally_data_path)
        for data in final_data:
            if not data.get("fisrtResult"):
                data["fisrtResult"] = "error"
            port_ids = safe_parse_value(data.get("firstPortId"), default=[])
            if not port_ids:
                continue
            record_docs = self._build_record_docs(port_ids)
            chat_result = await self._run_chat_flow(
                "gfChatApi_1",
                data,
                {"messages": f"生成{data.get('stockName')}的框架", "recordDocs": record_docs},
                replace_trace=True,
            )
            data["fisrtResult"] = chat_result.get("result") if chat_result.get("isSucess") and "result" in chat_result else chat_result if chat_result.get("isSucess") else "error"
        if is_save:
            write_excel_records(final_data, result_path)
        return final_data

    async def _update_with_report_type(self, data: dict[str, Any], report_type: list[int], agent_key: str = "gfChatApi_2"):
        port_ids = safe_parse_value(data.get("secondPortId"), default=[])
        record_docs = self._build_record_docs(port_ids, allow_types=report_type)
        target_field = f"updateResult_{'_'.join([str(item) for item in report_type])}"
        if not record_docs:
            data[target_field] = None
            return data
        chat_result = await self._run_chat_flow(
            agent_key,
            data,
            {
                "messages": f"更新{data.get('stockName')}的框架",
                "recordDocs": record_docs,
                "dynamicPrompt": {"framework": data.get("fisrtResult")},
            },
            replace_trace=True,
        )
        data[target_field] = chat_result.get("result") if chat_result.get("isSucess") and "result" in chat_result else chat_result if chat_result.get("isSucess") else "error"
        return data

    def update_result(self, finally_data_path: str, save_path: str | None = None, is_save: bool = False):
        return asyncio.run(self.update_result_async(finally_data_path=finally_data_path, save_path=save_path, is_save=is_save))

    async def update_result_async(self, finally_data_path: str, save_path: str | None = None, is_save: bool = False):
        result_path = Path(save_path) if save_path else self.path_manager.output_path("save_updateResult_path.xlsx")
        final_data = self._read_records(finally_data_path)
        for data in final_data:
            if data.get("fisrtResult", "error") == "error":
                continue
            second_port = safe_parse_value(data.get("secondPortId"), default=[])
            if not second_port:
                continue
            data = await self._update_with_report_type(data, [1])
            data = await self._update_with_report_type(data, [4])
            data = await self._update_with_report_type(data, [1, 4])
        if is_save:
            write_excel_records(final_data, result_path)
        return final_data

    @staticmethod
    def _pick_framework(data: dict[str, Any]) -> Any:
        return (
            data.get("updateResult_1_4")
            or data.get("updateResult_4")
            or data.get("updateResult_1")
            or data.get("firstResult")
            or data.get("fisrtResult")
        )

    def simple_result(self, finally_data_path: str, save_path: str | None = None, is_save: bool = False):
        return asyncio.run(self.simple_result_async(finally_data_path=finally_data_path, save_path=save_path, is_save=is_save))

    async def simple_result_async(self, finally_data_path: str, save_path: str | None = None, is_save: bool = False):
        result_path = Path(save_path) if save_path else self.path_manager.output_path("save_simpleResult_path.xlsx")
        final_data = self._read_records(finally_data_path)
        for data in final_data:
            if data.get("simpleResult") not in (None, "error"):
                continue
            first_port = safe_parse_value(data.get("firstPortId"), default=[])
            second_port = safe_parse_value(data.get("secondPortId"), default=[])
            port_ids = first_port + second_port
            if not any(item[1] == 4 for item in port_ids):
                continue
            financial_docs = self._build_record_docs(port_ids, allow_types=[4])
            if not financial_docs:
                continue
            framework = self._pick_framework(data)
            chat_result = await self._run_chat_flow(
                "gfChatApi_3",
                data,
                {
                    "messages": f"结合{data.get('stockName')}的新增素材，得到本次问答框架。",
                    "recordDocs": [financial_docs[-1]],
                    "dynamicPrompt": {"framework": framework},
                },
                replace_trace=True,
            )
            data["simpleResult"] = chat_result.get("result") if chat_result.get("isSucess") and "result" in chat_result else chat_result if chat_result.get("isSucess") else "error"
        if is_save:
            write_excel_records(final_data, result_path)
        return final_data

    def business_analysis(self, finally_data_path: str, save_path: str | None = None, is_save: bool = False, useDefault: bool = False):
        return asyncio.run(
            self.business_analysis_async(finally_data_path=finally_data_path, save_path=save_path, is_save=is_save, useDefault=useDefault)
        )

    async def business_analysis_async(self, finally_data_path: str, save_path: str | None = None, is_save: bool = False, useDefault: bool = False):
        result_path = Path(save_path) if save_path else self.path_manager.output_path("save_chatResult_path.xlsx")
        final_data = self._read_records(finally_data_path)
        for data in final_data:
            if not data.get("simpleResult"):
                continue
            first_port = safe_parse_value(data.get("firstPortId"), default=[])
            second_port = safe_parse_value(data.get("secondPortId"), default=[])
            port_ids = first_port + second_port
            financial_report_data = [item for item in port_ids if item[1] == 4]
            if not financial_report_data:
                continue
            record_docs = self._build_record_docs(financial_report_data)
            idx = random.choice(list(range(len(record_docs)))) if useDefault else -1
            record_doc = [record_docs[idx]]
            report_date = financial_report_data[idx][3]
            if report_date.endswith("-06-30"):
                report_type = "半年度"
            elif report_date.endswith("-12-31"):
                report_type = "年度"
            else:
                report_type = ""
            report_type = f"{report_date.split('-')[0]}年{report_type}"
            agent_update_params = {
                "messages": f"结合{data.get('stockName')}的新增素材, 撰写{data.get('stockName')}的{report_type}的业务分析",
                "recordDocs": record_doc,
            }
            if not useDefault:
                agent_update_params["dynamicPrompt"] = {"company_frame": data.get("simpleResult")}
            chat_result = await self._run_chat_flow(
                "gfChatApi_4",
                data,
                agent_update_params,
                replace_trace=True,
                not_tran_json=True,
            )
            target_data_field = "defaultResultData" if useDefault else "chatResultData"
            target_result_field = "defaultResult" if useDefault else "chatResult"
            data[target_data_field] = financial_report_data[idx]
            data[target_result_field] = chat_result.get("result") if chat_result.get("isSucess") else "error"
        if is_save:
            write_excel_records(final_data, result_path)
        return final_data
