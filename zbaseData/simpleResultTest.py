from __future__ import annotations

import argparse

from individualStockReview.metrics.summary import recallPerformance, summarize_simple_result


def recall_performance(pred, file):
    return recallPerformance(pred, file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="统计单份召回结果的 recall 与耗时")
    parser.add_argument("--input", required=True, help="召回结果 Excel 路径")
    parser.add_argument("--no-save-back", action="store_true", help="只统计不回写原文件")
    args = parser.parse_args()
    summarize_simple_result(result_path=args.input, save_back=not args.no_save_back)
