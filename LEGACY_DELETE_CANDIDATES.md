### Legacy 删除候选清单

当前重构阶段**不直接删除**旧目录能力；以下内容仅作为后续与你确认时使用的审计清单。

- **`dataDeal/getDataOri.py`**：未发现仓内引用，仍保留为历史对照实现；其职责已被 `DataPreparePipeline.collect_report_pairs_async` 与 `dataDeal/getData.py` 兼容包装覆盖。

### 已保留为兼容包装的入口

以下文件当前仍建议保留，用于兼容旧脚本与历史调用方式：

- **`dataDeal/getData.py`**：包装 `DataPreparePipeline`
- **`dataDeal/getResult.py`**：包装 `DataPreparePipeline` 的初稿、更新、简化、业务分析流程
- **`modelDeal/performance.py`**：包装 `ModelEvalPipeline`
- **`modelDeal/businessAnaly.py`**：包装 `ModelEvalPipeline`
- **`baseData/getQuery.py`**、**`selectQuery.py`**、**`quoteTest.py`**、**`chatAnswerTest.py`**：保留主流程类方法风格

### 建议的后续确认顺序

1. 先回归验证 `baseData`、`dataDeal`、`modelDeal` 旧入口是否还能产出兼容结果。
2. 若验证通过，优先确认是否删除 `dataDeal/getDataOri.py`。
3. 其余兼容包装入口暂不建议删除，除非明确不再需要旧脚本调用。
