from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List

import aiohttp
import asyncio

from individualStockReview.core.compat import build_select_query_result
from individualStockReview.pipelines.base_data import BaseDataPipeline


class getData:
    def __init__(self, apiFile="./individualStockReview/apiInfo.yaml", savePath="./result/finally_data_path.xlsx", isSave=True, parallelNum=1, *args, **kwargs):
        self.savePath = savePath
        self.parallelNum = parallelNum
        self.pipeline = BaseDataPipeline(api_file=apiFile, runtime_file=kwargs.get("runtimeFile"))

        data_path = kwargs.get("dataPath", "./individualStockReview/outputs/newData.xlsx")
        middle_path = kwargs.get("middlePath")
        self.totalData = self.pipeline.select_queries(
            data_path=data_path,
            save_path=savePath,
            middle_path=middle_path,
            parallel_num=parallelNum,
            is_save=isSave,
        )

    def updateParams(self, agentType):
        return self.pipeline.registry.get_gf_agent(agentType)

    @staticmethod
    async def getReportByFileName(response: aiohttp.ClientResponse, *args, **kwargs) -> str:
        result = await response.json()
        return result["data"]["rows"][0]["id"]

    async def bounded_getResultAsync(self, func, **kwargs):
        return await func(**kwargs)

    async def getFinallyData(self, defaultResult: dict | None = None, middlePath=None, *args, **kwargs) -> List:
        return await self.pipeline.select_queries_async(
            data_path=kwargs.get("dataPath", "./individualStockReview/outputs/newData.xlsx"),
            save_path=kwargs.get("savePath", self.savePath),
            middle_path=middlePath or kwargs.get("middlePath"),
            parallel_num=kwargs.get("parallelNum", max(self.parallelNum, 10)),
            is_save=kwargs.get("isSave", False),
        )


if __name__ == "__main__":
    _ = getData(parallelNum=1)
