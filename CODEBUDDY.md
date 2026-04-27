# CODEBUDDY.md This file provides guidance to CodeBuddy when working with code in this repository.

## 常用命令

- **安装依赖**: `python -m pip install -r /home/customTest/individualStockReview/requirements.txt`
  仓库没有 `pyproject.toml` 或 `setup.py`；依赖入口就是 `requirements.txt`。核心依赖包括 `aiohttp`、`pandas`、`openpyxl`、`langchain-openai`、`akshare`。

- **运行个股图谱评测**: `python /home/customTest/individualStockReview/run/stock_graph_eval.py --data-path /home/customTest/individualStockReview/inputs/stock_graph.sql --output /home/customTest/individualStockReview/outputs/stock_graph_eval.json --parallel-num 5 --run-mode resume`
  这是最直接的运行入口。脚本会读取 SQL、调用 `stockGraph` 智能体、持续写入 JSON，并在结束后导出同名 `.xlsx`。

- **运行 performance_json 模板评测**: `cd /home/customTest && python -m individualStockReview.pipelines.performance_template_eval --data-path /home/customTest/individualStockReview/inputs/模板数据汇总.csv --output /home/customTest/individualStockReview/outputs/performance_template_eval.json --parallel-num 5 --run-mode resume`
  通过 Volcengine + LangChain 对 `performance_json` 模板做批量评测。默认支持断点续跑，并额外生成 `.state.json` 与 `.xlsx`。

- **运行 performance 模板 badcase 校验**: `cd /home/customTest && python -m individualStockReview.pipelines.performance_template_eval_badcases --output-prefix /home/customTest/individualStockReview/outputs/performance_template_eval_badcases --parallel-num 1`
  先构造一组坏样本，再复用主评测逻辑做回归检查。输出包含输入 CSV、评测 JSON、检查 JSON 与检查 CSV。

- **汇总多次召回评测结果**: `cd /home/customTest && python -m individualStockReview.zbaseData.getResult --inputs run1.xlsx run2.xlsx --real-result real.xlsx --output /home/customTest/individualStockReview/outputs/testResult.xlsx`
  对多份召回结果做 precision / recall / 耗时汇总。脚本来自兼容层，但实际逻辑在 `metrics/summary.py`。

- **整合多次召回结果**: `cd /home/customTest && python -m individualStockReview.zbaseData.intergrateResult --inputs run1.xlsx run2.xlsx --real-result real.xlsx --output /home/customTest/individualStockReview/outputs/newData.xlsx`
  把多次召回结果合并成单份 Excel，供后续筛选和评估流程继续使用。

- **统计单份召回结果**: `cd /home/customTest && python -m individualStockReview.zbaseData.simpleResultTest --input /home/customTest/individualStockReview/outputs/some_result.xlsx`
  统计单份结果的 recall 与耗时；默认会把衍生列直接回写到原 Excel。

- **语法级冒烟检查**: `python -m compileall /home/customTest/individualStockReview`
  仓库未提交正式 lint / test 配置时，这是最安全的本地快速检查方式；它不会访问外部 API，但只能发现语法级问题。

## 仓库现状说明

- **构建/打包**: 未发现 `pyproject.toml`、`setup.py` 或其他打包配置；项目按脚本和模块直接运行。
- **Lint**: 未发现 `ruff.toml`、`.flake8`、`mypy.ini` 或同类配置。
- **测试**: 未发现 `tests/`、`pytest.ini`、`conftest.py` 或已提交的测试文件，因此仓库里没有可直接运行的“单个测试”命令。
- **文档/规则文件**: 生成本文件时，未发现仓库内的 `README.md`、`AGENTS.md`、`CLAUDE.md`、Cursor 规则或 Copilot 指令文件。

## 高层架构

这个仓库本质上是一个 **Python 评测与数据准备工具集**，不是传统 Web 服务。它围绕两条主线组织：

1. **GF 平台智能体链路**：通过 HTTP + SSE 调用广发/相关平台上的 agent，完成框架生成、召回验证、问答评估、个股图谱评测等。
2. **Volcengine + LangChain 链路**：通过 OpenAI 兼容接口访问火山模型，对 `performance_json` 模板做结构化评测。

整体可以按 **`core` → `agent` → `pipelines` → 兼容层/运行入口** 来理解。

### `core/`: 基础设施层

`core` 负责所有跨流程共用的底座能力。

- `core/config.py` 的 `ConfigManager` 是全仓库的配置入口。它默认加载：
  - `apiInfo.yaml`：GF 类接口与 agent 模板配置
  - `config/runtime.yaml`：目录、日志、兼容输出配置
  - `config/volcengine.yaml`，若不存在则回退到 `config/volcengine.example.yaml`
- `core/paths.py` 的 `PathManager` 统一管理 `inputs/`、`artifacts/`、`outputs/`、`logs/`、`result/`。`runtime.yaml` 里开启了 `mirror_to_result`，所以很多写入 `outputs/` 的文件会镜像复制到 `result/`。
- `core/io.py` 封装 Excel/JSON 的读写，并把输出镜像逻辑收口到一个地方。很多 pipeline 保存结果时都依赖这里，而不是自己直接操作文件。
- `core/logging.py` 统一日志初始化，并对 token / key 类字段做脱敏。调试 agent 请求时，优先看这里的摘要与脱敏策略。
- `core/compat.py`（虽然这里未单独展开）承担“旧字段结构兼容”的角色，很多 pipeline 在构造默认结果时依赖它。

### `agent/`: 调用抽象层

`agent` 负责把“一个评测/生成请求”变成可复用的 provider 调用。

- `agent/registry.py` 的 `AgentRegistry` 是所有 pipeline 的统一入口。它内部同时持有：
  - `GFPlatformProvider`
  - `VolcengineLangChainProvider`
- `agent/providers/gf_platform.py` 负责从 `apiInfo.yaml` 取出某个 GF agent 的模板参数，再构造 `GFChatAgent`。
- `agent/providers/volcengine_langchain.py` 负责从火山配置构造 `ChatOpenAI`，再包装成 `LangChainEvaluator`。
- `agent/runners/sse_runner.py` 是 GF 链路的核心。这里实现了：
  - SSE 分块解析
  - 引用链接替换
  - `conversationId` / `conversation_id` 的会话刷新策略
  - 真正的 `GFChatAgent.getChatResult()`

GF 链路的数据流基本是：
**pipeline → `AgentRegistry.get_gf_agent()` → `GFPlatformProvider` → `GFChatAgent.getChatResult()` → SSE 解析 → 结构化结果 dict**。

Volc 链路则是：
**pipeline → `AgentRegistry.get_volcengine_evaluator()` → `VolcengineLangChainProvider.build_chat_model()` → `LangChainEvaluator.evaluate()` → `LangChainRunner` → 结构化结果 dict**。

`agent/template.py` 是这两条链路之间的一个小型模板层，用于渲染 `{{ variable }}` 占位符；GF 请求体和 LangChain prompt 都会复用它。

### `pipelines/`: 业务流程层

真正的“项目逻辑”集中在 `pipelines/`。如果要修改行为，通常优先改这里，而不是去动兼容脚本。

- `pipelines/stock_graph_eval.py` 是当前最完整、最独立的 GF 评测脚本。
  - 先用 `StockGraphSqlReader` 从 SQL 文件中抽取 `stock_graph` 记录。
  - 对 `framework_json` / `graph_json` 做清洗，其中会把 `sample` 字段统一替换成 `{{sample}}`。
  - 并发调用 `stockGraph` agent。
  - 结果以 JSON 作为断点续跑状态源，再导出 Excel。
  - 支持 `resume` / `overwrite` 及 `resume-type`。

- `pipelines/performance_template_eval.py` 是当前 Volcengine 评测主流程，也是仓库里状态管理最复杂的 pipeline。
  - 从 CSV 读取模板数据。
  - 根据 `report_date` 与 `report_date_type` 组装 prompt 变量。
  - 调用 `LangChainEvaluator`，要求模型只返回 `score` 和 `reason`。
  - 保存两份 JSON：一份公开结果，一份更完整的 `.state.json` 内部状态。
  - 最后生成多 sheet 的 Excel（`overview`、`results`、`issues`）。
  - 这里还会记录输入摘要、模型输出指纹、疑似缓存命中信号等，用于排查重复输出或续跑问题。

- `pipelines/performance_template_eval_badcases.py` 不是独立评测引擎，而是主评测 pipeline 的回归样例生成器。它手工构造一批 badcase，再调用 `PerformanceTemplateEvalPipeline` 校验分数和原因是否符合预期。

- `pipelines/base_data.py` 是一组较老但仍在用的 GF 流程集合，覆盖：
  - `generate_queries`
  - `integrate_results`
  - `select_queries`
  - `quote_test`
  - `answer_test`
  - 以及 recall 汇总包装
  这些流程以 Excel/JSON 中间结果为主，很多老脚本仍然从这里走。

- `pipelines/data_prepare.py` 是“多阶段素材准备与框架生成”流程。
  - 先从报告接口与 ES 接口收集研报/财报配对
  - 再生成初稿框架
  - 再按报告类型更新框架
  - 再生成问答框架和业务分析
  这是老业务链路里最像“编排器”的一个 pipeline。

- `pipelines/langchain_eval.py` 是轻量通用版的 LangChain 批量评测器；适合任意 Excel 数据 + prompt 模板输入，不带 `performance_template_eval` 那么强的领域规则与状态管理。

- `pipelines/model_eval.py` 则更像特定业务评估器：先获取业绩数据，再走 GF 的生成 agent 和评分 agent 形成一套评估链。

`pipelines/__init__.py` 使用懒加载导出这些 Pipeline 类，所以外部代码更适合从 `individualStockReview.pipelines` 统一导入，而不是手写很多深路径导入。

### 兼容层与旧入口

`zbaseData/`、`zdataDeal/`、`zmodelDeal/`、`utils/` 基本都是 **兼容旧调用方式的薄包装层**：

- 它们保留了旧类名、旧函数名、旧脚本入口。
- 真实逻辑大多已经迁移到 `pipelines/`、`core/`、`agent/`。
- 如果要修复行为，优先改新层；只有在需要维持旧 API 形状时，才同步调整这些目录。

特别注意：`metrics/summary.py` 和部分兼容脚本会把统计列直接回写原 Excel，因此对历史结果文件做分析时要意识到它们是“会修改输入文件”的。

### 运行入口与修改优先级

- `run/stock_graph_eval.py` 是唯一显式处理 `sys.path` 的轻量入口包装，因此它最适合直接 `python path/to/script.py` 运行。
- 其他可执行文件更稳妥的方式是：在 `/home/customTest` 下使用 `python -m individualStockReview....`。
- 修改优先级通常应为：
  1. `pipelines/`
  2. `agent/`
  3. `core/`
  4. 兼容层目录

如果一个需求同时涉及“接口参数如何构造”和“结果怎样落盘”，通常前者在 `agent/` / `apiInfo.yaml`，后者在 `pipelines/` + `core/io.py` / `core/paths.py`。
