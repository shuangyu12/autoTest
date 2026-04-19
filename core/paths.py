from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from individualStockReview import PACKAGE_ROOT


@dataclass
class PathManager:
    root_dir: Path = PACKAGE_ROOT
    runtime_config: Mapping[str, Any] | None = None
    inputs_dir: Path = field(init=False)
    artifacts_dir: Path = field(init=False)
    outputs_dir: Path = field(init=False)
    logs_dir: Path = field(init=False)
    compat_result_dir: Path = field(init=False)
    mirror_to_result: bool = field(init=False)

    def __post_init__(self) -> None:
        runtime_config = dict(self.runtime_config or {})
        directories = dict(runtime_config.get("directories", {}))
        compatibility = dict(runtime_config.get("compatibility", {}))
        self.inputs_dir = self.root_dir / directories.get("inputs", "inputs")
        self.artifacts_dir = self.root_dir / directories.get("artifacts", "artifacts")
        self.outputs_dir = self.root_dir / directories.get("outputs", "outputs")
        self.logs_dir = self.root_dir / directories.get("logs", "logs")
        self.compat_result_dir = self.root_dir / directories.get("compat_result", "result")
        self.mirror_to_result = bool(compatibility.get("mirror_to_result", False))

    def ensure_directories(self) -> None:
        for path in [self.inputs_dir, self.artifacts_dir, self.outputs_dir, self.logs_dir, self.compat_result_dir]:
            path.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def ensure_parent(path: str | Path) -> Path:
        target = Path(path).expanduser()
        target.parent.mkdir(parents=True, exist_ok=True)
        return target

    @staticmethod
    def _resolve(path: str | Path) -> Path:
        return Path(path).expanduser().resolve(strict=False)

    def is_output_path(self, path: str | Path) -> bool:
        target = self._resolve(path)
        try:
            target.relative_to(self._resolve(self.outputs_dir))
            return True
        except ValueError:
            return False

    def build_compat_mirror_path(self, path: str | Path) -> Path | None:
        if not self.mirror_to_result or not self.is_output_path(path):
            return None
        target = self._resolve(path)
        relative_path = target.relative_to(self._resolve(self.outputs_dir))
        return self.ensure_parent(self.compat_result_dir / relative_path)

    def input_path(self, name: str | Path) -> Path:
        return self.ensure_parent(self.inputs_dir / Path(name))

    def artifact_path(self, name: str | Path) -> Path:
        return self.ensure_parent(self.artifacts_dir / Path(name))

    def output_path(self, name: str | Path) -> Path:
        return self.ensure_parent(self.outputs_dir / Path(name))

    def log_path(self, name: str | Path) -> Path:
        return self.ensure_parent(self.logs_dir / Path(name))

    def compat_result_path(self, name: str | Path) -> Path:
        return self.ensure_parent(self.compat_result_dir / Path(name))
