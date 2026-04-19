from __future__ import annotations

import argparse

from individualStockReview.pipelines.base_data import BaseDataPipeline


def integrate_results(path_list, real_result_path, save_path=None, apiFile="./individualStockReview/apiInfo.yaml"):
    pipeline = BaseDataPipeline(api_file=apiFile)
    return pipeline.integrate_results(path_list=path_list, real_result_path=real_result_path, save_path=save_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="整合多次召回结果")
    parser.add_argument("--inputs", nargs="+", required=True, help="多次召回结果 Excel 路径列表")
    parser.add_argument("--real-result", required=True, help="query 真实文件 Excel 路径")
    parser.add_argument("--output", default="./individualStockReview/outputs/newData.xlsx", help="整合后的输出路径")
    parser.add_argument("--api-file", default="./individualStockReview/apiInfo.yaml", help="API 配置路径")
    args = parser.parse_args()
    integrate_results(args.inputs, args.real_result, save_path=args.output, apiFile=args.api_file)
