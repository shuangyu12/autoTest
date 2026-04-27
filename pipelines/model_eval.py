from __future__ import annotations

import asyncio
import copy
import json
from pathlib import Path
from typing import Any

import requests

from individualStockReview.agent.registry import AgentRegistry
from individualStockReview.core.compat import build_model_eval_result
from individualStockReview.core.config import ConfigManager
from individualStockReview.core.io import write_excel_records
from individualStockReview.core.logging import get_logger
from individualStockReview.core.paths import PathManager

LOGGER = get_logger("pipelines.model_eval")


class ModelEvalPipeline:
    def __init__(self, api_file: str | None = None, runtime_file: str | None = None):
        self.config_manager = ConfigManager(api_file=api_file, runtime_file=runtime_file)
        self.path_manager = PathManager(runtime_config=self.config_manager.runtime_args)
        self.path_manager.ensure_directories()
        self.registry = AgentRegistry(api_file=api_file, runtime_file=runtime_file)

    @staticmethod
    async def get_stock_name_ak(ts_code: str):
        try:
            import akshare as ak  # pylint: disable=import-outside-toplevel

            code = ts_code.split(".")[0]
            stock_info_df = ak.stock_info_a_code_name()
            result = stock_info_df[stock_info_df["code"] == code]
            if not result.empty:
                return True, result.iloc[0]["name"]
            return False, "未找到对应公司"
        except Exception as exc:
            return False, f"查询失败：{str(exc)}"

    @staticmethod
    def get_data_mysql(size: int):
        params = {"page": 1, "page_size": size}
        result = requests.post(
            url="https://irmp-cot-uat.gf.com.cn/cloudapi/nlppreprocess/v2/stock_performance/merge",
            json=params,
            timeout=120,
        )
        result.raise_for_status()
        return result.json()["data"]

    async def _evaluate_single(self, idx: int, data_params: dict[str, Any]):
        default_result = build_model_eval_result()
        relation_dict = {"1": "一季报", "2": "年报和一季报", "3": "半年报", "4": "三季报", "5": "年报"}
        report_date_type = str(data_params["report_date_type"])
        report_date = data_params["report_date"]
        time_desc = f"{int(report_date.split('-')[0])}年{relation_dict[report_date_type]}"
        stock_name = await self.get_stock_name_ak(data_params["security_code"])
        stock_name = stock_name[1] if stock_name[0] else data_params["security_code"]

        default_result.update(
            {
                "idx": idx,
                "messages": f"撰写{stock_name}公司的{time_desc}的业绩表现",
                "mergedTemplate": json.dumps(data_params.get("performance_json", {}), ensure_ascii=False),
            }
        )

        generation_agent = self.registry.get_gf_agent("gfChatApi_8")
        generation_result = await generation_agent.getChatResult(
            defaultResult=copy.deepcopy(default_result),
            agentUpdateParams={
                "messages": default_result["messages"],
                "dynamicPrompt": {"history_report_template": default_result["mergedTemplate"]},
            },
            notTranJson=True,
            replaceTrace=True,
        )
        if not generation_result.get("isSucess"):
            generation_result["errorInfo"] = generation_result.get("errorInfo", "")
            return generation_result

        score_agent = self.registry.get_gf_agent("testChatApi_8")
        score_result = await score_agent.getChatResult(
            defaultResult=copy.deepcopy(generation_result),
            agentUpdateParams={
                "messages": f"result:{generation_result.get('result', generation_result.get('chatResult', ''))}",
                "dynamicPrompt": {"reportDate": time_desc},
            },
            replaceTrace=True,
        )
        if score_result.get("isSucess"):
            score_result["chatResult"] = generation_result.get("result", generation_result.get("chatResult", ""))
        return score_result

    def evaluate(self, size: int = 100000000, save_path: str | None = None, parallel_num: int = 5, source_data: list[dict[str, Any]] | None = None):
        return asyncio.run(self.evaluate_async(size=size, save_path=save_path, parallel_num=parallel_num, source_data=source_data))

    async def evaluate_async(self, size: int = 100000000, save_path: str | None = None, parallel_num: int = 5, source_data: list[dict[str, Any]] | None = None):
        result_path = Path(save_path) if save_path else self.path_manager.output_path("save_business_performance.xlsx")
        data = source_data if source_data is not None else self.get_data_mysql(size=size)
        semaphore = asyncio.Semaphore(parallel_num)

        async def bounded(task_idx: int, params: dict[str, Any]):
            async with semaphore:
                return await self._evaluate_single(task_idx, params)

        tasks = []
        for idx, item in enumerate(data):
            item = copy.deepcopy(item)
            item.pop("create_time", None)
            item.pop("update_time", None)
            tasks.append(bounded(idx, item))

        total_result = await asyncio.gather(*tasks) if tasks else []
        total_result = [result for result in total_result if result.get("isSucess")]
        total_result = sorted(total_result, key=lambda item: item["idx"])
        write_excel_records(total_result, result_path)
        LOGGER.info("业绩表现评测完成，共输出 %s 条记录", len(total_result))
        return total_result
