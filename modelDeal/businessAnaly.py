from __future__ import annotations

import asyncio
from copy import deepcopy
from typing import Dict

from individualStockReview.core.compat import build_model_eval_result, ensure_legacy_defaults
from individualStockReview.core.logging import get_logger
from individualStockReview.pipelines.model_eval import ModelEvalPipeline

LOGGER = get_logger("modelDeal.businessAnaly")


async def get_stock_name_ak(ts_code):
    return await ModelEvalPipeline.get_stock_name_ak(ts_code)


async def getResultAsync(apiFile: str = "./individualStockReview/apiInfo.yaml", defaultResult: Dict | None = None, *args, **kwargs) -> Dict:
    pipeline = ModelEvalPipeline(api_file=apiFile, runtime_file=kwargs.get("runtimeFile"))

    payload = ensure_legacy_defaults(defaultResult or {}, build_model_eval_result())
    params = deepcopy(kwargs)
    idx = params.get("idx", payload.get("idx", 0))
    result = await pipeline._evaluate_single(idx, params)
    payload.update(result)
    return payload


def get_data_mysql(size):
    return ModelEvalPipeline.get_data_mysql(size)


async def mainAsync():
    pipeline = ModelEvalPipeline()
    result = await pipeline.evaluate_async(save_path="./individualStockReview/result/save_business_performance.xlsx")
    print("异步执行完成，结果已保存。")
    return result


if __name__ == "__main__":
    asyncio.run(mainAsync())
