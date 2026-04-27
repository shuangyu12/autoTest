from __future__ import annotations

import argparse
import json
from pathlib import Path

from individualStockReview.pipelines.performance_template_eval import build_argument_parser as build_main_parser
from individualStockReview.pipelines.performance_template_eval import main as run_main

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_PATH = str(PROJECT_ROOT / "outputs" / "performance_template_eval_badcases.json")


def build_argument_parser() -> argparse.ArgumentParser:
    parser = build_main_parser()
    parser.description = "performance_template_eval 的 badcase 兼容入口（自动开启人工评分模式）"
    parser.set_defaults(output=DEFAULT_OUTPUT_PATH, badcase_validation=True)
    return parser


def main(args: argparse.Namespace | None = None) -> dict:
    arguments = args or build_argument_parser().parse_args()
    arguments.badcase_validation = True
    return run_main(arguments)


if __name__ == "__main__":
    result = main()
    print(json.dumps(result.get("summary", {}), ensure_ascii=False, indent=2))
