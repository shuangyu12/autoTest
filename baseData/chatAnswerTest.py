from __future__ import annotations

from typing import List

from individualStockReview.pipelines.base_data import BaseDataPipeline


class getData:
    def __init__(self, apiFile="./individualStockReview/apiInfo.yaml", savePath="./individualStockReview/result/new_accurate_path_default.xlsx", isSave=True, parallelNum=1, *args, **kwargs):
        self.savePath = savePath
        self.parallelNum = parallelNum
        self.pipeline = BaseDataPipeline(api_file=apiFile, runtime_file=kwargs.get("runtimeFile"))

        self.getAnswerAgent = self.updateParams("reportAnswerTest")
        self.totalData = self.pipeline.answer_test(
            data_path=kwargs.get("dataPath", "./individualStockReview/outputs/percision_path_restore.xlsx"),
            save_path=savePath,
            parallel_num=parallelNum,
            is_save=isSave,
        )

    def updateParams(self, agentType):
        return self.pipeline.registry.get_gf_agent(agentType)

    async def bounded_getResultAsync(self, func, **kwargs):
        return await func(**kwargs)

    async def getFinallyData(self, defaultResult: dict | None = None, *args, **kwargs) -> List:
        return await self.pipeline.answer_test_async(
            data_path=kwargs.get("dataPath", "./individualStockReview/outputs/percision_path_restore.xlsx"),
            save_path=kwargs.get("savePath", self.savePath),
            parallel_num=kwargs.get("parallelNum", self.parallelNum),
            is_save=kwargs.get("isSave", False),
        )


if __name__ == "__main__":
    _ = getData(parallelNum=5)
