from __future__ import annotations

from typing import List

from individualStockReview.pipelines.langchain_eval import LangChainEvalPipeline


class getData:
    def __init__(
        self,
        apiFile="./individualStockReview/apiInfo.yaml",
        savePath="./individualStockReview/result/save_volc_langchain_eval.xlsx",
        isSave=True,
        parallelNum=1,
        *args,
        **kwargs,
    ):
        self.savePath = savePath
        self.parallelNum = parallelNum
        self.pipeline = LangChainEvalPipeline(
            api_file=apiFile,
            runtime_file=kwargs.get("runtimeFile"),
            volcengine_file=kwargs.get("volcengineFile"),
        )
        self.totalData = self.pipeline.evaluate(
            prompt_template=kwargs.get("promptTemplate"),
            system_prompt=kwargs.get("systemPrompt"),
            data_path=kwargs.get("dataPath"),
            source_data=kwargs.get("sourceData"),
            save_path=savePath,
            parallel_num=parallelNum,
            is_save=isSave,
            template_variables=kwargs.get("templateVariables"),
            provider_config=kwargs.get("providerConfig"),
            sheet_name=kwargs.get("sheetName"),
            parse_json=kwargs.get("parseJson", True),
            keep_missing=kwargs.get("keepMissing", True),
        )

    async def bounded_getResultAsync(self, func, **kwargs):
        return await func(**kwargs)

    async def getFinallyData(self, defaultResult: dict | None = None, *args, **kwargs) -> List:
        return await self.pipeline.evaluate_async(
            prompt_template=kwargs.get("promptTemplate"),
            system_prompt=kwargs.get("systemPrompt"),
            data_path=kwargs.get("dataPath"),
            source_data=kwargs.get("sourceData"),
            save_path=kwargs.get("savePath", self.savePath),
            parallel_num=kwargs.get("parallelNum", self.parallelNum),
            is_save=kwargs.get("isSave", False),
            template_variables=kwargs.get("templateVariables"),
            provider_config=kwargs.get("providerConfig"),
            sheet_name=kwargs.get("sheetName"),
            parse_json=kwargs.get("parseJson", True),
            keep_missing=kwargs.get("keepMissing", True),
        )


if __name__ == "__main__":
    _ = getData(parallelNum=1)
