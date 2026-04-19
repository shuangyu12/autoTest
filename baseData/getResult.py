from __future__ import annotations

import argparse

from individualStockReview.metrics.summary import percisionAndRecall, summarize_recall_results


def percision_and_recall(realFile, predList, realList):
    return percisionAndRecall(realFile, predList, realList)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="汇总多次评测的 precision/recall/耗时结果")
    parser.add_argument("--inputs", nargs="+", required=True, help="待汇总的评测 Excel 路径列表")
    parser.add_argument("--real-result", required=True, help="真实标签 Excel 路径")
    parser.add_argument("--output", default="./individualStockReview/outputs/testResult.xlsx", help="汇总输出路径")
    args = parser.parse_args()
    summarize_recall_results(path_list=args.inputs, real_result_path=args.real_result, save_path=args.output)
