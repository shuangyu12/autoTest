from __future__ import annotations

import ast
import json
import shutil
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd

from .config import ConfigManager
from .logging import get_logger, summarize_data
from .paths import PathManager

LOGGER = get_logger("core.io")


@lru_cache(maxsize=1)
def _get_runtime_path_manager() -> PathManager:
    config_manager = ConfigManager()
    manager = PathManager(runtime_config=config_manager.runtime_args)
    manager.ensure_directories()
    return manager


def _mirror_written_file(target: Path, mirror_to_compat: bool | None = None) -> None:
    path_manager = _get_runtime_path_manager()
    if mirror_to_compat is False:
        return
    if mirror_to_compat is None and not path_manager.mirror_to_result:
        return
    compat_target = path_manager.build_compat_mirror_path(target)
    if compat_target is None:
        return
    if compat_target.resolve(strict=False) == target.resolve(strict=False):
        return
    shutil.copy2(target, compat_target)
    LOGGER.debug("兼容结果镜像写入: %s", summarize_data({"source": str(target), "target": str(compat_target)}))


def safe_parse_value(value: Any, default: Any = None) -> Any:
    if value is None:
        return default
    if isinstance(value, float) and pd.isna(value):
        return default
    if not isinstance(value, str):
        return value

    text = value.strip()
    if not text:
        return default

    for parser in (json.loads, ast.literal_eval):
        try:
            return parser(text)
        except Exception:
            continue
    return value


def read_excel_records(path: str | Path, **kwargs: Any) -> list[dict[str, Any]]:
    frame = pd.read_excel(path, usecols=lambda col: not str(col).startswith("Unnamed"), **kwargs)
    return [frame.iloc[idx, :].to_dict() for idx in range(len(frame))]


def write_excel_records(
    records: list[dict[str, Any]] | pd.DataFrame,
    path: str | Path,
    index: bool = False,
    mirror_to_compat: bool | None = None,
) -> Path:
    target = PathManager.ensure_parent(path)
    frame = records if isinstance(records, pd.DataFrame) else pd.DataFrame(records)
    frame.to_excel(target, index=index)
    _mirror_written_file(target, mirror_to_compat=mirror_to_compat)
    return target


def read_json_file(path: str | Path, default: Any = None) -> Any:
    file_path = Path(path)
    if not file_path.exists():
        return default
    with file_path.open("r", encoding="utf-8") as file:
        return json.load(file)


def write_json_file(path: str | Path, data: Any, mirror_to_compat: bool | None = False) -> Path:
    target = PathManager.ensure_parent(path)
    with target.open("w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)
    _mirror_written_file(target, mirror_to_compat=mirror_to_compat)
    return target
