from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

from individualStockReview.pipelines.performance_template_eval import (
    PerformanceTemplateEvalPipeline,
    _build_provider_config_from_args,
)


DEFAULT_OUTPUT_PREFIX = "/home/customTest/individualStockReview/outputs/performance_template_eval_badcases"


def _make_template_text(text: str) -> str:
    return json.dumps([
        {
            "id": "0-1",
            "label": "财务表现",
            "content": text,
        }
    ], ensure_ascii=False)


def build_badcase_specs() -> list[dict[str, Any]]:
    base_record = {
        "security_code": "000001.SZ",
        "industry_names": "测试行业",
        "report_date": "2025-03-31",
        "report_date_type": "1",
        "sort_no": 1,
    }
    return [
        {
            **base_record,
            "id": "bc_garbled_text",
            "case_name": "局部乱码难分离",
            "error_type": "乱码",
            "acceptable_scores": [0.7, 0.4],
            "expected_reason_keywords": ["乱码", "异常字符", "难以填充"],
            "performance_json": _make_template_text(
                "公司发布{N}年一季报。{N}Q1实现营业总收入-亿元，同比-；归母净利润-亿元，同比-；毛利率å¤§æ¦为-%，同比��pct。"
            ),
        },
        {
            **base_record,
            "id": "bc_hard_to_fill",
            "case_name": "占位符难填写",
            "error_type": "难填写",
            "acceptable_scores": [0.7, 0.4],
            "expected_reason_keywords": ["yoy", "??", "难以填充", "难以直接填充"],
            "performance_json": _make_template_text(
                "公司发布{N}年一季报。{N}Q1实现营业总收入-亿元，yoy+-%；归母净利润-亿元，(yoy+-%)；扣非净利润-亿元，同比增长??。"
            ),
        },
        {
            **base_record,
            "id": "bc_fixed_numbers",
            "case_name": "写死具体数值",
            "error_type": "具体数值",
            "acceptable_scores": [0.7, 0.4],
            "expected_reason_keywords": ["数值", "写死", "具体数值"],
            "performance_json": _make_template_text(
                "公司发布{N}年一季报。{N}Q1实现营业总收入-亿元，同比-；归母净利润-亿元，同比-；销售费用率12.3%，同比-pct。"
            ),
        },
        {
            **base_record,
            "id": "bc_subjective_conclusion",
            "case_name": "主观具体结论",
            "error_type": "具体结论",
            "acceptable_scores": [0.7, 0.4],
            "expected_reason_keywords": ["结论", "主观", "推荐", "拐点"],
            "performance_json": _make_template_text(
                "公司发布{N}年一季报。{N}Q1实现营业总收入-亿元，同比-；归母净利润-亿元，同比-；扣非净利润-亿元，同比-。毛利率-%，同比-pct；经营现金流净额-亿元，同比增长-%。公司盈利能力显著改善，基本面拐点已经确立，维持强烈推荐。"
            ),
        },
        {
            **base_record,
            "id": "bc_multiple_issues",
            "case_name": "多处明确问题但结构仍可辨认",
            "error_type": "多问题",
            "acceptable_scores": [0.4],
            "expected_reason_keywords": ["多处", "乱码", "yoy", "??", "数值"],
            "performance_json": _make_template_text(
                "公司发布{N}年一季报。{N}Q1实现营业总收入88.2亿元，yoy+-%；归母净利润-亿元，同比增长??；毛利率å¤§æ¦为-%。"
            ),
        },
        {
            **base_record,
            "id": "bc_wrong_period",
            "case_name": "报告期错乱",
            "error_type": "报告期错乱",
            "acceptable_scores": [0.0],
            "expected_reason_keywords": ["报告期", "错乱", "年报", "H1", "Q4"],
            "performance_json": _make_template_text(
                "公司发布{N}年年报。{N}全年实现营业总收入-亿元，同比-；{N}Q4归母净利润-亿元，同比-；{N}H1毛利率-%，同比-pct。"
            ),
        },
        {
            **base_record,
            "id": "bc_many_issues",
            "case_name": "大量问题混合",
            "error_type": "大量问题",
            "acceptable_scores": [0.0],
            "expected_reason_keywords": ["报告期", "乱码", "yoy", "结论", "无关"],
            "performance_json": _make_template_text(
                "公司发布2024年年报。2024年实现营业收入123.46亿元，同比增长18.7%；{N}Q1归母净利润-亿元，yoy+-%；毛利率å¤§æ¦为-%，同比??pct。盈利拐点确立，建议积极增持。附：联系人张三，电话123456。"
            ),
        },
    ]


def _build_input_rows(specs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    keys = [
        "id",
        "security_code",
        "industry_names",
        "report_date",
        "report_date_type",
        "sort_no",
        "performance_json",
    ]
    return [{key: spec.get(key) for key in keys} for spec in specs]


def _reason_keyword_hit(reason: str, keywords: list[str]) -> bool:
    if not keywords:
        return True
    lowered_reason = reason.lower()
    return any(keyword.lower() in lowered_reason for keyword in keywords)


def _build_check_payload(specs: list[dict[str, Any]], eval_payload: dict[str, Any]) -> dict[str, Any]:
    result_by_id = {
        str(item.get("id")): item
        for item in (eval_payload.get("results") or [])
        if isinstance(item, dict) and item.get("id")
    }
    rows: list[dict[str, Any]] = []
    for spec in specs:
        actual = result_by_id.get(spec["id"], {})
        actual_score = float(actual.get("评分", 0.0)) if actual else 0.0
        actual_reason = str(actual.get("评分原因") or "")
        acceptable_scores = [float(score) for score in (spec.get("acceptable_scores") or [])]
        score_match = any(abs(actual_score - score) < 1e-9 for score in acceptable_scores)
        keyword_hit = _reason_keyword_hit(actual_reason, list(spec.get("expected_reason_keywords") or []))
        passed = bool(actual.get("isSucess")) and score_match and keyword_hit
        rows.append(
            {
                "id": spec["id"],
                "case_name": spec["case_name"],
                "error_type": spec["error_type"],
                "acceptable_scores": acceptable_scores,
                "actual_score": actual_score,
                "score_match": score_match,
                "reason_keyword_hit": keyword_hit,
                "passed": passed,
                "actual_reason": actual_reason,
                "expected_reason_keywords": list(spec.get("expected_reason_keywords") or []),
                "eval_error": str(actual.get("errorInfo") or ""),
            }
        )

    passed_rows = [row for row in rows if row["passed"]]
    failed_rows = [row for row in rows if not row["passed"]]
    return {
        "summary": {
            "total_cases": len(rows),
            "passed_cases": len(passed_rows),
            "failed_cases": len(failed_rows),
            "pass_case_ids": [row["id"] for row in passed_rows],
            "failed_case_ids": [row["id"] for row in failed_rows],
        },
        "results": rows,
    }


def run_badcase_eval(args: argparse.Namespace) -> dict[str, Any]:
    output_prefix = Path(args.output_prefix or DEFAULT_OUTPUT_PREFIX)
    output_prefix.parent.mkdir(parents=True, exist_ok=True)
    input_csv_path = output_prefix.with_suffix(".input.csv")
    eval_json_path = output_prefix.with_suffix(".json")
    check_json_path = output_prefix.with_suffix(".check.json")
    check_csv_path = output_prefix.with_suffix(".check.csv")

    specs = build_badcase_specs()
    pd.DataFrame(_build_input_rows(specs)).to_csv(input_csv_path, index=False)

    pipeline = PerformanceTemplateEvalPipeline(
        api_file=args.api_file,
        runtime_file=args.runtime_file,
        volcengine_file=args.volcengine_file,
    )
    eval_payload = pipeline.evaluate(
        data_path=str(input_csv_path),
        save_path=str(eval_json_path),
        parallel_num=max(args.parallel_num, 1),
        save_every=1,
        provider_config=_build_provider_config_from_args(args),
        resume=False,
    )

    check_payload = _build_check_payload(specs=specs, eval_payload=eval_payload)
    check_payload["meta"] = {
        "input_csv_path": str(input_csv_path),
        "eval_json_path": str(eval_json_path),
        "check_csv_path": str(check_csv_path),
        "generated_at": pd.Timestamp.now().isoformat(timespec="seconds"),
    }

    with check_json_path.open("w", encoding="utf-8") as file:
        json.dump(check_payload, file, ensure_ascii=False, indent=2)
    pd.DataFrame(check_payload["results"]).to_csv(check_csv_path, index=False)
    return check_payload


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="构造精简 badcase 并复用 performance_template_eval 提示词做判别校验")
    parser.add_argument("--output-prefix", default=DEFAULT_OUTPUT_PREFIX, help="输出前缀，不带后缀")
    parser.add_argument("--parallel-num", type=int, default=1, help="并发评测数，默认 1 以控制 token 与调用波动")
    parser.add_argument("--api-file", default=None, help="apiInfo.yaml 路径")
    parser.add_argument("--runtime-file", default=None, help="runtime.yaml 路径")
    parser.add_argument("--volcengine-file", default=None, help="volcengine.yaml 路径")
    parser.add_argument("--provider", default=None, help="provider 名称，默认 volcengine")
    parser.add_argument("--base-url", default=None, help="火山 Ark OpenAI 兼容地址")
    parser.add_argument("--api-key", default=None, help="火山 Ark API Key")
    parser.add_argument("--model", default=None, help="火山模型 ID")
    parser.add_argument("--temperature", type=float, default=None, help="模型温度")
    parser.add_argument("--timeout", type=float, default=None, help="模型超时时间")
    parser.add_argument("--max-retries", type=int, default=None, help="兼容参数；底层仍由主评测脚本统一关闭 SDK 重试")
    return parser


if __name__ == "__main__":
    arguments = build_argument_parser().parse_args()
    result = run_badcase_eval(arguments)
    print(json.dumps(result.get("summary", {}), ensure_ascii=False, indent=2))
