from __future__ import annotations

import asyncio
from copy import deepcopy
from pathlib import Path
from typing import Any

from individualStockReview.agent.registry import AgentRegistry
from individualStockReview.core.compat import build_langchain_eval_result, ensure_legacy_defaults
from individualStockReview.core.config import ConfigManager
from individualStockReview.core.io import read_excel_records, write_excel_records
from individualStockReview.core.logging import get_logger
from individualStockReview.core.paths import PathManager

LOGGER = get_logger("pipelines.langchain_eval")


class LangChainEvalPipeline:
    def __init__(
        self,
        api_file: str | None = None,
        runtime_file: str | None = None,
        volcengine_file: str | None = None,
    ):
        self.config_manager = ConfigManager(api_file=api_file, runtime_file=runtime_file, volcengine_file=volcengine_file)
        self.path_manager = PathManager(runtime_config=self.config_manager.runtime_args)
        self.path_manager.ensure_directories()
        self.registry = AgentRegistry(api_file=api_file, runtime_file=runtime_file, volcengine_file=volcengine_file)
        self.logger = get_logger("pipelines.langchain_eval")

    def evaluate(
        self,
        prompt_template: str | None = None,
        system_prompt: str | None = None,
        data_path: str | None = None,
        source_data: list[dict[str, Any]] | None = None,
        save_path: str | None = None,
        parallel_num: int = 5,
        is_save: bool = True,
        template_variables: dict[str, Any] | None = None,
        provider_config: dict[str, Any] | None = None,
        sheet_name: str | None = None,
        parse_json: bool = True,
        keep_missing: bool = True,
    ):
        return asyncio.run(
            self.evaluate_async(
                prompt_template=prompt_template,
                system_prompt=system_prompt,
                data_path=data_path,
                source_data=source_data,
                save_path=save_path,
                parallel_num=parallel_num,
                is_save=is_save,
                template_variables=template_variables,
                provider_config=provider_config,
                sheet_name=sheet_name,
                parse_json=parse_json,
                keep_missing=keep_missing,
            )
        )

    async def evaluate_async(
        self,
        prompt_template: str | None = None,
        system_prompt: str | None = None,
        data_path: str | None = None,
        source_data: list[dict[str, Any]] | None = None,
        save_path: str | None = None,
        parallel_num: int = 5,
        is_save: bool = True,
        template_variables: dict[str, Any] | None = None,
        provider_config: dict[str, Any] | None = None,
        sheet_name: str | None = None,
        parse_json: bool = True,
        keep_missing: bool = True,
    ):
        result_path = Path(save_path) if save_path else self.path_manager.output_path("save_volc_langchain_eval.xlsx")
        data_list = source_data if source_data is not None else read_excel_records(data_path, sheet_name=sheet_name) if data_path else []
        evaluator = self.registry.get_volcengine_evaluator(override_config=provider_config)
        semaphore = asyncio.Semaphore(parallel_num)

        async def bounded(task_idx: int, record: dict[str, Any]):
            async with semaphore:
                default_result = ensure_legacy_defaults(record, build_langchain_eval_result())
                default_result["idx"] = record.get("idx", task_idx)
                effective_prompt_template = prompt_template or record.get("promptTemplate") or record.get("prompt_template") or record.get("messages")
                effective_system_prompt = system_prompt if system_prompt is not None else record.get("systemPrompt") or record.get("system_prompt")
                record_variables = deepcopy(template_variables or {})
                record_variables.update(record)
                return await evaluator.evaluate(
                    default_result=default_result,
                    prompt_template=effective_prompt_template,
                    system_prompt=effective_system_prompt,
                    template_variables=record_variables,
                    parse_json=parse_json,
                    keep_missing=keep_missing,
                )

        tasks = [bounded(idx, deepcopy(item)) for idx, item in enumerate(data_list)]
        total_result = await asyncio.gather(*tasks) if tasks else []
        if is_save:
            write_excel_records(total_result, result_path)
        LOGGER.info("Volcengine LangChain 评测完成，共输出 %s 条记录", len(total_result))
        return total_result
