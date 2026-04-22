from __future__ import annotations

from copy import deepcopy
from typing import Dict, List

import asyncio

from individualStockReview.core.compat import build_quote_result
from individualStockReview.pipelines.base_data import BaseDataPipeline


class getData:
    def __init__(self, apiFile="./individualStockReview/apiInfo.yaml", savePath="./individualStockReview/result/percision_path_restore.xlsx", isSave=True, parallelNum=1, *args, **kwargs):
        self.savePath = savePath
        self.parallelNum = parallelNum
        self.pipeline = BaseDataPipeline(api_file=apiFile, runtime_file=kwargs.get("runtimeFile"))

        self.getAnswerAgent = self.updateParams("getAnswerCustom")
        self.quoteTestAgent = self.updateParams("quoteTest")
        self.totalData = self.pipeline.quote_test(
            data_path=kwargs.get("dataPath", "./individualStockReview/inputs/研报问答.xlsx"),
            save_path=savePath,
            middle_path=kwargs.get("middlePath"),
            parallel_num=parallelNum,
            inner_parallel_num=kwargs.get("innerParallelNum", 6),
            is_save=isSave,
            sheet_name=kwargs.get("sheetName", "研报问答-广发模式"),
        )
        self.innerResult = []
        for data in self.totalData:
            self.innerResult.extend(data[1])
        self.totalData = [data[0] for data in self.totalData]

    def updateParams(self, agentType):
        return self.pipeline.registry.get_gf_agent(agentType)

    async def bounded_getResultAsync(self, func, **kwargs):
        return await func(**kwargs)

    async def singleQuoteTest(self, defaultResult: dict | None = None, *args, **kwargs) -> Dict:
        payload = deepcopy(defaultResult) if defaultResult is not None else build_quote_result()
        payload["quote"] = kwargs.get("quoteIdx", payload.get("quote"))
        return await self.pipeline._score_single_quote(self.quoteTestAgent, payload)

    async def quoteTest(self, defaultResult: dict | None = None, *args, **kwargs) -> Dict:
        payload = deepcopy(defaultResult) if defaultResult is not None else build_quote_result()
        return await self.pipeline._evaluate_quote_bundle(
            self.quoteTestAgent,
            payload,
            inner_parallel_num=kwargs.get("innerParallelNum", 6),
        )

    async def getFinallyData(self, defaultResult: dict | None = None, *args, **kwargs) -> List:
        return await self.pipeline.quote_test_async(
            data_path=kwargs.get("dataPath", "./individualStockReview/inputs/研报问答.xlsx"),
            save_path=kwargs.get("savePath", self.savePath),
            middle_path=kwargs.get("middlePath"),
            parallel_num=kwargs.get("parallelNum", self.parallelNum),
            inner_parallel_num=kwargs.get("innerParallelNum", 6),
            is_save=kwargs.get("isSave", False),
            sheet_name=kwargs.get("sheetName", "研报问答-广发模式"),
        )


if __name__ == "__main__":
    _ = getData(parallelNum=1)
