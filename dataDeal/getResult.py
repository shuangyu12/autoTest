from __future__ import annotations

import asyncio
from typing import Any, List

import aiohttp

from individualStockReview.agent.runners.sse_runner import GFChatAgent, _parse_single_sse_event, parse_sse_chunks, replace_chat_link
from individualStockReview.core.logging import get_logger
from individualStockReview.pipelines.data_prepare import DataPreparePipeline
from individualStockReview.utils.base import retryClass

LOGGER = get_logger("dataDeal.getResult")


class getBaseResult:
    def __init__(self, apiFile="./individualStockReview/apiInfo.yaml", sessionName="gfGetSession", chatName="gfChatApi_1", *args, **kwargs):
        self.apiFile = apiFile
        self.pipeline = DataPreparePipeline(api_file=apiFile, runtime_file=kwargs.get("runtimeFile"))
        self.logger = get_logger("dataDeal.getResult.base")

    async def getData(self, dataPath, resultName, isRetry=False, default="error", isSave=True, savePath="", *args, **kwargs):
        dispatcher = {
            "fisrtResult": self.pipeline.first_result_async,
            "simpleResult": self.pipeline.simple_result_async,
        }
        if resultName not in dispatcher:
            raise ValueError(f"暂不支持的兼容 resultName: {resultName}")
        result = await dispatcher[resultName](
            finally_data_path=dataPath,
            save_path=savePath or None,
            is_save=isSave,
        )
        self.totalData = result
        return result

    @staticmethod
    async def wrap_task(func, idx=None, *args, **kwargs):
        result = await func(*args, **kwargs)
        return {
            "func_name": func.__name__ if idx is None else idx,
            "result": result,
        }

    async def getChatResult(self, data, resultName, *args, **kwargs):
        raise NotImplementedError("旧 getBaseResult.getChatResult 已由 DataPreparePipeline 接管")

    def getFinallyResult(self, *args, **kwargs):
        return None


async def jsonAnswerAsync(response: aiohttp.ClientResponse, *args, **kwargs) -> dict[str, Any] | str:
    return await GFChatAgent.jsonAnswerAsync(response, *args, **kwargs)


def _legacy_save_path(kwargs: dict[str, Any], default_name: str) -> str | None:
    if "save_path" in kwargs and kwargs.get("save_path") is not None:
        return kwargs.get("save_path")
    if kwargs.get("is_save", False):
        return default_name
    return None


async def firstResult(apiFile: str = "./individualStockReview/apiInfo.yaml", finallyDataPath: str = "./result/save_firstResult_path.xlsx", *args, **kwargs) -> List:
    pipeline = DataPreparePipeline(api_file=apiFile, runtime_file=kwargs.get("runtimeFile"))
    return await pipeline.first_result_async(
        finally_data_path=finallyDataPath,
        save_path=_legacy_save_path(kwargs, "./save_firstResult_path.xlsx"),
        is_save=kwargs.get("is_save", False),
    )


async def update(data, sessionParam, agentChatParam, reportType: List = [1], *args, **kwargs):
    pipeline = DataPreparePipeline(
        api_file=kwargs.get("apiFile", "./individualStockReview/apiInfo.yaml"),
        runtime_file=kwargs.get("runtimeFile"),
    )
    return await pipeline._update_with_report_type(data, reportType)



async def updateResult(apiFile: str = "./individualStockReview/apiInfo.yaml", finallyDataPath: str = "./save_firstResult_path.xlsx", *args, **kwargs) -> List:
    pipeline = DataPreparePipeline(api_file=apiFile, runtime_file=kwargs.get("runtimeFile"))
    return await pipeline.update_result_async(
        finally_data_path=finallyDataPath,
        save_path=_legacy_save_path(kwargs, "./save_updateResult_path.xlsx"),
        is_save=kwargs.get("is_save", False),
    )


async def simpleResult(apiFile: str = "./individualStockReview/apiInfo.yaml", finallyDataPath: str = "./save_updateResult_path.xlsx", *args, **kwargs) -> List:
    pipeline = DataPreparePipeline(api_file=apiFile, runtime_file=kwargs.get("runtimeFile"))
    return await pipeline.simple_result_async(
        finally_data_path=finallyDataPath,
        save_path=_legacy_save_path(kwargs, "./save_simpleResult_path.xlsx"),
        is_save=kwargs.get("is_save", False),
    )


async def businessAnalysis(apiFile: str = "./individualStockReview/apiInfo.yaml", finallyDataPath: str = "./save_simpleResult_path.xlsx", *args, **kwargs) -> List:
    pipeline = DataPreparePipeline(api_file=apiFile, runtime_file=kwargs.get("runtimeFile"))
    return await pipeline.business_analysis_async(
        finally_data_path=finallyDataPath,
        save_path=_legacy_save_path(kwargs, "./save_chatResult_path.xlsx"),
        is_save=kwargs.get("is_save", False),
        useDefault=kwargs.get("useDefault", False),
    )


async def mainAsync():
    result = await businessAnalysis(is_save=True, finallyDataPath="./save_chatResult_path.xlsx", useDefault=False)
    print("异步执行完成，结果已保存。")
    return result


if __name__ == "__main__":
    asyncio.run(mainAsync())
