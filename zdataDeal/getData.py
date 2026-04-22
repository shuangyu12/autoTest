from __future__ import annotations

from typing import Any, Dict, List

import aiohttp
import asyncio

from individualStockReview.core.logging import get_logger
from individualStockReview.pipelines.data_prepare import DataPreparePipeline
from individualStockReview.utils.base import retryClass as _retryClass

LOGGER = get_logger("dataDeal.getData")


class retryClass(_retryClass):
    pass


class getData:
    def __init__(self, apiFile="./individualStockReview/apiInfo.yaml", savePath="./result/save_data_path.xlsx", *args, **kwargs):
        super().__init__()
        self.savePath = savePath
        self.pipeline = DataPreparePipeline(api_file=apiFile, runtime_file=kwargs.get("runtimeFile"))
        self.totalData = asyncio.run(
            self.bounded_getResultAsync(
                self.getFinallyData,
                isSave=kwargs.get("isSave", True),
                includeFilePaths=kwargs.get("includeFilePaths", False),
                requireResearchReport=kwargs.get("requireResearchReport", False),
            )
        )

    @staticmethod
    async def wrap_task(func, idx=None, *args, **kwargs):
        result = await func(*args, **kwargs)
        return {
            "func_name": func.__name__ if idx is None else idx,
            "result": result,
        }

    @staticmethod
    async def getReportInfo(response: aiohttp.ClientResponse, *args, **kwargs) -> List:
        result = await response.json()
        return result["data"]["rows"]

    @staticmethod
    async def getEsInfo(response: aiohttp.ClientResponse, *args, **kwargs) -> List:
        result = await response.json()
        return result.get("data", {}).get("recordList", [])

    async def getResearchReportData(self, *args, **kwargs) -> Dict[str, list[dict[str, Any]]]:
        return await self.pipeline._collect_reports(
            "getResearchReport",
            "knowledge_base_content_report",
            include_sub_type_filter=True,
        )

    async def getFinancialReportData(self, *args, **kwargs) -> Dict[str, list[dict[str, Any]]]:
        return await self.pipeline._collect_reports(
            "getFinancialReport",
            "knowledge_base_content_financial_report",
            include_sub_type_filter=False,
        )

    @staticmethod
    async def bounded_getResultAsync(func, parallelNum=1, **kwargs):
        semaphore = asyncio.Semaphore(parallelNum)
        async with semaphore:
            return await func(**kwargs)

    async def getFinallyData(self, defaultResult: dict | None = None, *args, **kwargs) -> List:
        return await self.pipeline.collect_report_pairs_async(
            save_path=kwargs.get("savePath", self.savePath),
            include_file_paths=kwargs.get("includeFilePaths", False),
            require_research_report=kwargs.get("requireResearchReport", False),
            is_save=kwargs.get("isSave", False),
        )


if __name__ == "__main__":
    totalData = getData().totalData
