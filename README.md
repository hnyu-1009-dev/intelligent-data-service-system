# intelligent-data-service-system 

## 项目简介

`intelligent-data-service-system` 是一个面向业务分析场景的数据问答系统。  
它的目标不是做通用聊天，而是把自然语言问题转换成可执行的数据查询流程，并将执行阶段、查询结果或错误信息以流式方式返回给前端页面。

从代码实现上看，这个项目围绕“自然语言查询 -> 元数据召回 -> SQL 生成与校验 -> 数据仓库执行 -> 前端展示”这一条链路展开，适合用于销售、订单、客户、商品、区域、时间等主题域的指标查询与分析问答。

---

## 项目用来做什么

这个项目主要解决的是业务人员或产品人员不直接编写 SQL 的情况下，依然能够通过自然语言完成数据查询的问题。

它当前承载的能力包括：

- 接收用户输入的自然语言业务问题。
- 基于元数据和向量检索理解问题中涉及的表、字段、指标和值。
- 结合数据库信息、日期上下文和提示词生成 SQL。
- 对 SQL 做校验和必要纠错，再落到数据仓库执行。
- 通过 SSE 将阶段进度、结果表格和错误信息持续推送给前端。

从 `conf/meta_config.yaml` 和相关模型设计可以看出，当前项目的业务建模偏向典型数仓分析场景，例如：

- 维度表：区域、客户、商品、日期。
- 事实表：订单。
- 指标：GMV、AOV 等聚合指标。

因此，这个系统本质上是一个“面向分析型数仓的 NL2SQL 数据问答系统”，而不是普通的聊天机器人。

---

## 总体架构

项目采用前后端分离结构，整体可以拆成四层：

1. 展示层  
   Vue 3 前端提供单页问答界面，负责输入问题、接收流式响应、展示阶段进度、结果表格和异常信息。

2. 接口层  
   FastAPI 提供 `/api/query` 接口，并以 `text/event-stream` 形式将 LangGraph 执行过程持续输出到前端。

3. Agent 编排层  
   LangGraph 将一次查询拆成关键词抽取、检索召回、上下文补全、SQL 生成、SQL 校验、SQL 修正、SQL 执行等多个节点，组成完整工作流。

4. 数据与知识层  
   MySQL、Qdrant、Elasticsearch、Embedding 服务共同承担结构化数据执行、元数据管理、语义召回和值检索能力。

可以将它理解为下面这条主链路：

`前端问题输入 -> FastAPI -> ChatService -> LangGraph -> 元数据/向量/全文检索 -> SQL 生成与校验 -> DW 执行 -> SSE 回传前端`

---

## 架构拆解

### 1. 前端架构

前端位于 `date-agent-frontend` 目录，使用 Vue 3 + Vite 构建，职责比较明确：

- 提供问答输入界面。
- 调用 `/api/query` 接口。
- 以流式方式接收服务端返回的 `stage`、`result`、`error`。
- 将结果渲染为步骤列表、表格和错误提示。

前端本身不承担查询理解与数据处理逻辑，它更像一个实时查询控制台。

### 2. 后端架构

后端位于 `data-agent` 目录，采用 FastAPI 作为服务入口。

后端的主干结构是：

- `main.py`：应用入口，注册路由和中间件。
- `app/api`：接口层与依赖注入。
- `app/service`：服务编排层。
- `app/agent`：LangGraph 工作流及节点逻辑。
- `app/repositories`：数据访问抽象。
- `app/clients`：底层客户端管理。
- `app/config`：配置加载。
- `app/scripts`：元知识构建脚本。

其中最核心的不是单个接口，而是 `app/agent/graph.py` 中定义的查询图。

### 3. 数据与知识架构

项目没有直接把“数据库查询”做成简单的问答，而是引入了多种存储与检索组件协同工作：

- `db_dw`：数据仓库，承接最终 SQL 执行。
- `db_meta`：元数据库，保存表、字段、指标及关联关系。
- Qdrant：保存字段和指标的向量化知识，用于语义召回。
- Elasticsearch：保存字段值样本，用于值检索和实体命中。
- Embedding 服务：将字段名、别名、描述、指标等转换为向量。
- LLM：负责关键词提取、SQL 生成、过滤与纠错。

这套设计说明项目不仅依赖大模型，还把结构化元数据和检索系统放在了同等重要的位置。

---

## 核心实现技术

### 后端技术栈

- FastAPI：对外提供接口和流式响应能力。
- LangGraph：编排多节点 Agent 工作流。
- LangChain：承接 LLM、PromptTemplate、OutputParser 等能力。
- SQLAlchemy + asyncmy：异步访问 MySQL。
- Qdrant：字段/指标语义召回。
- Elasticsearch：字段值检索。
- HuggingFace Embedding Endpoint：生成向量表示。
- Loguru：统一日志输出。
- OmegaConf / YAML：配置与元知识定义加载。

### 前端技术栈

- Vue 3：界面与状态管理。
- Vite：前端开发与构建。
- 原生 `fetch + ReadableStream`：接收后端 SSE 流式返回。

### 工程实现方式

项目并没有把所有逻辑堆在接口里，而是遵循了比较清晰的职责分层：

- `clients` 管理底层连接。
- `repositories` 负责数据读写。
- `service` 负责对工作流和仓储的组合调用。
- `agent/nodes` 负责一个个原子步骤。
- `graph.py` 负责定义步骤顺序和条件流转。

这让系统既能保持 Agent 式的灵活性，也能保留传统服务端项目的可维护性。

---

## 业务逻辑与执行链路

### 1. 查询入口

前端将用户问题提交到 `/api/query`。  
后端在 `chat_router.py` 中接收请求，并返回 `StreamingResponse`，将后续执行过程以 SSE 形式输出。

### 2. 服务编排

`ChatService` 会构建：

- `DataAgentState`：保存这次查询的状态数据。
- `DataAgentContext`：注入仓储、Embedding 客户端等运行时依赖。

然后把 `state + context` 送入 LangGraph 执行。

### 3. LangGraph 工作流

从 `app/agent/graph.py` 可以看到，当前查询流程被拆成以下节点：

- `extract_keywords`
- `column_recall`
- `value_recall`
- `metric_recall`
- `merge_retrieved_info`
- `filter_table_info`
- `filter_metric_info`
- `add_context`
- `generate_sql`
- `validate_sql`
- `correct_sql`
- `execute_sql`

这条链路表达的业务含义是：

1. 从自然语言问题中抽取关键词。
2. 分别从字段、字段值、指标三个维度做召回。
3. 合并召回结果，得到候选表结构与候选指标。
4. 过滤出真正与本次问题相关的表和指标。
5. 补充日期信息、数据库信息等上下文。
6. 生成 SQL。
7. 先校验 SQL 是否可执行、是否合理。
8. 如果校验失败，则进入 SQL 修正节点。
9. SQL 通过后，交由数据仓库执行。
10. 将执行结果实时返回前端。

这说明项目不是“一步到位让模型写 SQL”，而是使用多阶段检索和约束来降低 SQL 生成风险。

### 4. 元知识构建逻辑

项目还包含一条离线知识构建链路，对应 `app/scripts/build_meta_knowledge.py` 和 `app/service/meta_knowledge_service.py`。

这条链路的作用是：

- 从 YAML 元配置读取表、字段、指标定义。
- 将表结构和字段定义写入 Meta MySQL。
- 为字段和指标构建向量并写入 Qdrant。
- 将需要做值检索的字段样本同步到 Elasticsearch。

也就是说，在线查询依赖的“知识底座”并不是临时生成的，而是通过离线构建流程预先准备好的。

### 5. 流式反馈逻辑

系统在节点执行过程中会通过 `runtime.stream_writer` 不断写出阶段信息。  
前端收到的数据大体分为三类：

- `stage`：执行进度。
- `result`：最终结果表格。
- `error`：异常信息。

这种设计对业务用户更友好，因为用户能看到系统正在做什么，而不是长时间等待一个最终结果。

---

## 项目中的关键角色

### FastAPI

负责把整个 Agent 能力暴露成可调用接口，并承担请求生命周期管理、中间件和流式返回。

### LangGraph

是这个项目的核心编排引擎。  
它决定一次查询会经历哪些步骤、步骤间如何衔接、SQL 校验失败后如何进入纠错分支。

### Meta MySQL

保存业务表、字段、指标及其关系，是系统理解数仓结构的基础。

### DW MySQL

作为最终 SQL 执行目标，承接真实分析查询。

### Qdrant

负责做“字段/指标语义召回”。  
当用户问题里使用的是业务同义词、口语描述或别名时，Qdrant 可以帮助找到对应字段或指标。

### Elasticsearch

负责做“值检索”。  
例如用户问题里提到某个地区、某个品牌、某类客户时，ES 能帮助系统匹配具体取值。

### Embedding 服务

负责将字段、指标及其描述向量化，为 Qdrant 检索提供基础。

### LLM

承担高层语义理解和 SQL 生成任务，但它不是单独完成所有工作，而是在元数据约束和检索结果辅助下参与决策。

---

## 目录树

下面的目录树只保留对理解项目架构有价值的部分：

```text
intelligent-data-service-system/
├─ README.md
├─ data-agent/
│  ├─ main.py
│  ├─ pyproject.toml
│  ├─ conf/
│  │  ├─ app_config.yaml
│  │  └─ meta_config.yaml
│  ├─ prompts/
│  │  ├─ generate_sql.prompt
│  │  ├─ correct_sql.prompt
│  │  ├─ filter_table_info.prompt
│  │  ├─ filter_metric_info.prompt
│  │  └─ ...
│  ├─ docker/
│  │  ├─ docker-compose.yaml
│  │  ├─ mysql/
│  │  ├─ elasticsearch/
│  │  └─ embedding/
│  └─ app/
│     ├─ api/
│     │  ├─ deps.py
│     │  └─ routers/
│     │     └─ chat_router.py
│     ├─ agent/
│     │  ├─ context.py
│     │  ├─ graph.py
│     │  ├─ llm.py
│     │  ├─ state.py
│     │  └─ nodes/
│     │     ├─ extract_keywords.py
│     │     ├─ column_recall.py
│     │     ├─ value_recall.py
│     │     ├─ metric_recall.py
│     │     ├─ merge_retrieved_info.py
│     │     ├─ filter_table_info.py
│     │     ├─ filter_metric_info.py
│     │     ├─ add_context.py
│     │     ├─ generate_sql.py
│     │     ├─ validate_sql.py
│     │     ├─ correct_sql.py
│     │     └─ execute_sql.py
│     ├─ clients/
│     │  ├─ mysql_client.py
│     │  ├─ es_client.py
│     │  ├─ qdrant_client_manager.py
│     │  └─ embedding_client.py
│     ├─ config/
│     │  ├─ app_config.py
│     │  ├─ meta_config.py
│     │  └─ config_loader.py
│     ├─ core/
│     │  ├─ lifespan.py
│     │  ├─ logging.py
│     │  ├─ middleware.py
│     │  └─ context.py
│     ├─ models/
│     │  ├─ mysql/
│     │  ├─ qdrant/
│     │  └─ es/
│     ├─ prompt/
│     │  └─ prompt_loader.py
│     ├─ repositories/
│     │  ├─ mysql/
│     │  ├─ qdrant/
│     │  └─ es/
│     ├─ schemas/
│     │  └─ chat.py
│     ├─ scripts/
│     │  └─ build_meta_knowledge.py
│     └─ service/
│        ├─ chat_service.py
│        └─ meta_knowledge_service.py
└─ date-agent-frontend/
   ├─ package.json
   ├─ vite.config.js
   ├─ index.html
   ├─ public/
   └─ src/
      ├─ main.js
      ├─ style.css
      ├─ App.vue
      ├─ assets/
      └─ components/
```

---

## 总结

这个项目的核心价值不在于“做了一个聊天页面”，而在于把数仓元数据、向量检索、全文检索、LLM 推理和 SQL 执行串成了一条可落地的数据问答链路。

从工程设计上看，它已经具备了一个分析型 Agent 系统的典型特征：

- 前后端分离。
- 在线查询与离线知识构建分离。
- 检索与生成结合。
- SQL 生成、校验、纠错、执行分阶段处理。
- 流式反馈用户执行过程。

如果后续继续演进，这个项目很自然可以往更复杂的企业级数据问答平台扩展，例如：

- 更复杂的指标体系。
- 多主题域支持。
- 权限与审计。
- 查询结果缓存。
- 对话上下文记忆。
- 更精细的 SQL 风险控制。
