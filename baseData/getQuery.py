from __future__ import annotations

from copy import deepcopy
from typing import List

import aiohttp
import asyncio

from individualStockReview.core.compat import build_query_result
from individualStockReview.pipelines.base_data import BaseDataPipeline


class getData:
    def __init__(self, apiFile="./individualStockReview/apiInfo.yaml", savePath="./result/save_data_path.xlsx", isSave=True, parallelNum=1, *args, **kwargs):
        self.savePath = savePath
        self.parallelNum = parallelNum
        self.pipeline = BaseDataPipeline(api_file=apiFile, runtime_file=kwargs.get("runtimeFile"))

        self.totalData = self.pipeline.generate_queries(
            save_path=savePath,
            is_save=isSave,
            parallel_num=parallelNum,
            limit=kwargs.get("limit", 50),
        )

    @staticmethod
    async def getReportInfo(response: aiohttp.ClientResponse, *args, **kwargs) -> List:
        result = await response.json()
        return result["data"]["rows"]

    async def bounded_getResultAsync(self, func, **kwargs):
        return await func(**kwargs)

    async def getChatResult(self, defaultResult: dict | None = None, *args, **kwargs) -> dict:
        default_result = deepcopy(defaultResult) if defaultResult is not None else build_query_result()
        agent = self.pipeline.registry.get_gf_agent("queryGenerate")
        agent_update_params = kwargs.get(
            "agentUpdateParams",
            {
                "messages": "参考研报, 设计出一个全面、简洁、具有强相关性的query。",
                "recordDocs": [{"docName": default_result["fileName"] or "研报标题", "docId": default_result["fileId"]}],
            },
        )
        return await agent.getChatResult(defaultResult=default_result, agentUpdateParams=agent_update_params, replaceTrace=True)

    async def getFinallyData(self, defaultResult: dict | None = None, *args, **kwargs) -> List:
        return await self.pipeline.generate_queries_async(
            save_path=kwargs.get("savePath", self.savePath),
            is_save=kwargs.get("isSave", False),
            parallel_num=kwargs.get("parallelNum", self.parallelNum),
            limit=kwargs.get("limit", 50),
        )


if __name__ == "__main__":
    _ = getData(parallelNum=5)
