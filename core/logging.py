from __future__ import annotations

import logging
import re
from typing import Any

from .config import ConfigManager
from .paths import PathManager

TOKEN_PATTERN = re.compile(r"([A-Za-z0-9_-]{8})[A-Za-z0-9_-]{8,}([A-Za-z0-9_-]{4})")
SENSITIVE_KEYS = {
    "authorization",
    "authcode",
    "api_key",
    "apikey",
    "token",
    "access_token",
    "refresh_token",
    "secret",
    "password",
}
SUMMARY_KEYS = {
    "messages",
    "prompt",
    "result",
    "chatresult",
    "reason",
    "content",
    "answer",
    "querydsl",
}
COLLECTION_KEYS = {
    "recorddocs",
    "urls",
    "ref_list",
    "takenumlist",
    "splitresult",
    "custom_inputs",
    "dynamicprompt",
    "dynamic_prompt",
}
_LOGGER_CONFIGURED = False


def _mask_text(text: str) -> str:
    return TOKEN_PATTERN.sub(r"\1****\2", text)


def _truncate_text(text: str, max_length: int = 240) -> str:
    if len(text) <= max_length:
        return text
    return f"{text[:max_length]}...<截断 {len(text) - max_length} 字符>"


def summarize_data(data: Any, max_items: int = 5, max_text_length: int = 240) -> Any:
    if isinstance(data, dict):
        summary: dict[str, Any] = {}
        for idx, (key, value) in enumerate(data.items()):
            if idx >= max_items:
                summary["..."] = f"还有 {len(data) - max_items} 个字段"
                break
            lower_key = str(key).lower()
            if lower_key in COLLECTION_KEYS:
                if isinstance(value, (list, tuple, set)):
                    summary[key] = f"<{type(value).__name__} size={len(value)}>"
                elif isinstance(value, dict):
                    summary[key] = f"<dict size={len(value)}>"
                else:
                    summary[key] = summarize_data(value, max_items=max_items, max_text_length=max_text_length)
                continue
            summary[key] = summarize_data(value, max_items=max_items, max_text_length=max_text_length)
            if lower_key in SUMMARY_KEYS and isinstance(summary[key], str):
                summary[key] = _truncate_text(summary[key], max_length=max_text_length)
        return mask_sensitive_data(summary)
    if isinstance(data, list):
        items = [summarize_data(item, max_items=max_items, max_text_length=max_text_length) for item in data[:max_items]]
        if len(data) > max_items:
            items.append(f"...还有 {len(data) - max_items} 项")
        return items
    if isinstance(data, tuple):
        return tuple(summarize_data(list(data), max_items=max_items, max_text_length=max_text_length))
    if isinstance(data, str):
        return _truncate_text(_mask_text(data), max_length=max_text_length)
    return data


def mask_sensitive_data(data: Any) -> Any:
    if isinstance(data, dict):
        return {
            key: ("****" if str(key).lower() in SENSITIVE_KEYS else mask_sensitive_data(value))
            for key, value in data.items()
        }
    if isinstance(data, list):
        return [mask_sensitive_data(item) for item in data]
    if isinstance(data, tuple):
        return tuple(mask_sensitive_data(item) for item in data)
    if isinstance(data, str):
        return _mask_text(data)
    return data


class SensitiveFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        message = super().format(record)
        return _mask_text(message)


def setup_logging(force: bool = False) -> logging.Logger:
    global _LOGGER_CONFIGURED
    config_manager = ConfigManager()
    path_manager = PathManager(runtime_config=config_manager.runtime_args)
    path_manager.ensure_directories()

    logger_name = config_manager.get_runtime("logging", "logger_name", default="individualStockReview")
    logger_level = config_manager.get_runtime("logging", "level", default="DEBUG")
    console_level = config_manager.get_runtime("logging", "console_level", default="INFO")
    file_level = config_manager.get_runtime("logging", "file_level", default="DEBUG")
    console_format = config_manager.get_runtime("logging", "console_format", default="%(levelname)s %(message)s")
    file_format = config_manager.get_runtime(
        "logging",
        "file_format",
        default="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    log_file_name = config_manager.get_runtime("logging", "file_name", default="individualStockReview.log")
    console_enabled = bool(config_manager.get_runtime("logging", "console_enabled", default=True))
    file_enabled = bool(config_manager.get_runtime("logging", "file_enabled", default=True))

    logger = logging.getLogger(logger_name)
    logger.setLevel(getattr(logging, str(logger_level).upper(), logging.DEBUG))
    logger.propagate = False

    if force:
        logger.handlers.clear()
        _LOGGER_CONFIGURED = False

    if not _LOGGER_CONFIGURED:
        if console_enabled:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(getattr(logging, str(console_level).upper(), logging.INFO))
            console_handler.setFormatter(SensitiveFormatter(console_format))
            logger.addHandler(console_handler)

        if file_enabled:
            file_handler = logging.FileHandler(path_manager.log_path(log_file_name), encoding="utf-8")
            file_handler.setLevel(getattr(logging, str(file_level).upper(), logging.DEBUG))
            file_handler.setFormatter(SensitiveFormatter(file_format))
            logger.addHandler(file_handler)

        _LOGGER_CONFIGURED = True

    return logger


def get_logger(name: str | None = None) -> logging.Logger:
    logger = setup_logging()
    return logger.getChild(name) if name else logger
