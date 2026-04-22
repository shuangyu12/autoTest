from __future__ import annotations

from typing import Any

__all__ = [
    "BaseDataPipeline",
    "DataPreparePipeline",
    "LangChainEvalPipeline",
    "ModelEvalPipeline",
    "PerformanceTemplateEvalPipeline",
    "StockGraphEvalPipeline",
]


def __getattr__(name: str) -> Any:
    if name == "BaseDataPipeline":
        from .base_data import BaseDataPipeline

        return BaseDataPipeline
    if name == "DataPreparePipeline":
        from .data_prepare import DataPreparePipeline

        return DataPreparePipeline
    if name == "LangChainEvalPipeline":
        from .langchain_eval import LangChainEvalPipeline

        return LangChainEvalPipeline
    if name == "ModelEvalPipeline":
        from .model_eval import ModelEvalPipeline

        return ModelEvalPipeline
    if name == "PerformanceTemplateEvalPipeline":
        from .performance_template_eval import PerformanceTemplateEvalPipeline

        return PerformanceTemplateEvalPipeline
    if name == "StockGraphEvalPipeline":
        from .stock_graph_eval import StockGraphEvalPipeline

        return StockGraphEvalPipeline
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
