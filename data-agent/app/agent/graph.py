# asyncio：
# Python 标准库中的异步编程模块。
# 这段代码里虽然没有直接使用 asyncio.run(main())，
# 但通常异步主流程会通过 asyncio 启动。
import asyncio

# START / END：
# 来自 LangGraph 的两个特殊节点常量。
# - START：图执行的起点
# - END：图执行的终点
from langgraph.constants import START, END

# StateGraph：
# LangGraph 中用于构建“有状态工作流图”的核心类。
# 这里会基于：
# - state_schema：状态结构
# - context_schema：上下文结构
# 创建一个可以串联多个节点的执行图。
from langgraph.graph import StateGraph

# DataAgentContext：
# 项目定义的运行时上下文类型。
# 里面通常放“节点执行时需要的外部依赖”，例如：
# - Qdrant Repository
# - ES Repository
# - Embedding Client
# - MySQL Repository
from app.agent.context import DataAgentContext

# 下面这些都是图中的“节点函数”。
# 每个节点负责一段独立逻辑，多个节点串起来形成完整的智能问数流程。
from app.agent.nodes.add_context import add_context
from app.agent.nodes.column_recall import column_recall
from app.agent.nodes.correct_sql import correct_sql
from app.agent.nodes.execute_sql import execute_sql
from app.agent.nodes.extract_keywords import extract_keywords
from app.agent.nodes.filter_metric_info import filter_metric_info
from app.agent.nodes.filter_table_info import filter_table_info
from app.agent.nodes.generate_sql import generate_sql
from app.agent.nodes.merge_retrieved_info import merge_retrieved_info
from app.agent.nodes.metric_recall import metric_recall
from app.agent.nodes.validate_sql import validate_sql
from app.agent.nodes.value_recall import value_recall

# DataAgentState：
# 项目定义的状态类型。
# 它表示图执行过程中不断流转和更新的数据，
# 例如：
# - query：用户原始问题
# - keywords：提取出的关键词
# - retrieved_columns：召回到的字段
# - sql：生成的 SQL
# - error：SQL 校验错误
from app.agent.state import DataAgentState

# 下面这些 manager 是项目中统一管理底层客户端资源的对象。
# 它们的职责通常是：
# - init()：初始化底层 client / engine
# - close()：释放资源
from app.clients.embedding_client import embedding_client_manager
from app.clients.es_client import es_client_manager
from app.clients.mysql_client import meta_client_manager, dw_client_manager
from app.clients.qdrant_client_manager import qdrant_client_manager

# request_id_ctx_var：
# 项目里的上下文变量，通常用于日志追踪。
# 给当前请求设置一个 request_id 后，后续日志就能带上同一个标识，
# 便于排查问题。
from app.core.context import request_id_ctx_var

# Repository 层：
# 用于封装各类底层数据访问逻辑。
from app.repositories.es.value_es_repository import ValueESRepository
from app.repositories.mysql.dw_mysql_repository import DWMySQLRepository
from app.repositories.mysql.meta_mysql_repository import MetaMySQLRepository
from app.repositories.qdrant.column_repository_qdrant import ColumnQdrantRepository
from app.repositories.qdrant.metric_repository_qdrant import MetricQdrantRepository

# 创建一个 LangGraph 状态图构建器。
#
# 这里指定了两类 schema：
# - state_schema=DataAgentState
#   表示整张图中流转的“状态”长什么样
# - context_schema=DataAgentContext
#   表示图运行时传入的“上下文依赖”长什么样
graph_builder = StateGraph(state_schema=DataAgentState, context_schema=DataAgentContext)

# 向图中注册节点。
# 每个节点对应一个独立的处理步骤。
#
# 从整体上看，这个图大概是一个 NL2SQL / 智能问数流程：
# 1. 提取关键词
# 2. 召回字段
# 3. 召回字段值
# 4. 召回指标
# 5. 合并召回结果
# 6. 过滤表 / 指标
# 7. 补充上下文
# 8. 生成 SQL
# 9. 校验 SQL
# 10. 修正 SQL（如果失败）
# 11. 执行 SQL
graph_builder.add_node("extract_keywords", extract_keywords)
graph_builder.add_node("column_recall", column_recall)
graph_builder.add_node("value_recall", value_recall)
graph_builder.add_node("metric_recall", metric_recall)
graph_builder.add_node("merge_retrieved_info", merge_retrieved_info)
graph_builder.add_node("filter_table_info", filter_table_info)
graph_builder.add_node("filter_metric_info", filter_metric_info)
graph_builder.add_node("add_context", add_context)
graph_builder.add_node("generate_sql", generate_sql)
graph_builder.add_node("validate_sql", validate_sql)
graph_builder.add_node("correct_sql", correct_sql)
graph_builder.add_node("execute_sql", execute_sql)

# 定义图中节点之间的执行顺序。
#
# 这部分可以理解成“流程编排”：
# START -> extract_keywords
# extract_keywords -> column_recall / value_recall / metric_recall（并行召回）
# 召回完成后 -> merge_retrieved_info
# merge 后 -> filter_table_info / filter_metric_info
# 再 -> add_context -> generate_sql -> validate_sql
# 最后根据校验结果决定执行还是修正 SQL
graph_builder.add_edge(START, "extract_keywords")
graph_builder.add_edge("extract_keywords", "column_recall")
graph_builder.add_edge("extract_keywords", "value_recall")
graph_builder.add_edge("extract_keywords", "metric_recall")
graph_builder.add_edge("value_recall", "merge_retrieved_info")
graph_builder.add_edge("column_recall", "merge_retrieved_info")
graph_builder.add_edge("metric_recall", "merge_retrieved_info")
graph_builder.add_edge("merge_retrieved_info", "filter_table_info")
graph_builder.add_edge("merge_retrieved_info", "filter_metric_info")
graph_builder.add_edge("filter_table_info", "add_context")
graph_builder.add_edge("filter_metric_info", "add_context")
graph_builder.add_edge("add_context", "generate_sql")
graph_builder.add_edge("generate_sql", "validate_sql")

# 这条边和下面的 conditional_edges 有一点重复语义：
# validate_sql -> execute_sql
#
# 因为你后面又定义了条件分支：
# - 如果没有错误 -> execute_sql
# - 如果有错误 -> correct_sql
#
# 从设计角度看，通常只保留 conditional_edges 会更清晰。
graph_builder.add_edge("validate_sql", "execute_sql")

# 定义条件分支边。
#
# 这里表示：
# - 如果 state["error"] is None，说明 SQL 校验通过，走 execute_sql
# - 否则走 correct_sql，让模型或逻辑去修正 SQL
graph_builder.add_conditional_edges(
    "validate_sql",
    lambda state: "execute_sql" if state["error"] is None else "correct_sql",
    {
        "execute_sql": "execute_sql",
        "correct_sql": "correct_sql",
    },
)

# 执行 SQL 后流程结束
graph_builder.add_edge("execute_sql", END)

# 编译图，得到真正可执行的 graph 对象。
graph = graph_builder.compile()


async def main():
    """
    main() 是一个演示 / 本地测试入口。

    它做的事情大概分 6 步：
    1. 设置 request_id
    2. 构造初始 state（用户问题）
    3. 初始化 Qdrant / Embedding / ES / MySQL 客户端
    4. 创建 Repository 对象
    5. 组装 DataAgentContext
    6. 执行 graph，并流式打印执行过程中的输出
    """

    # 设置当前请求的 request_id。
    # 日志里如果使用了这个上下文变量，就能把整次调用串起来。
    request_id_ctx_var.set("1")

    # 构造图的初始状态。
    # 这里只传入一个 query，表示用户原始问题。
    state = DataAgentState(query="统计一下2025年1月份各品类的销售额占比")

    # 初始化 Qdrant 客户端管理器。
    # init() 后 qdrant_client_manager.client 才可用。
    qdrant_client_manager.init()

    # 基于 Qdrant client 创建字段 / 指标的向量仓储对象。
    column_qdrant_repository = ColumnQdrantRepository(qdrant_client_manager.client)
    metric_qdrant_repository = MetricQdrantRepository(qdrant_client_manager.client)

    # 初始化 Embedding 客户端。
    embedding_client_manager.init()
    embedding_client = embedding_client_manager.client

    # 初始化 Elasticsearch 客户端，并创建值检索仓储对象。
    es_client_manager.init()
    value_es_repository = ValueESRepository(es_client_manager.client)

    # 初始化 MySQL 客户端管理器。
    meta_client_manager.init()
    dw_client_manager.init()

    # 创建两个异步数据库 session：
    # - meta_session：连接元数据库
    # - dw_session：连接数据仓库
    #
    # async with 的作用是自动管理 session 生命周期，用完自动关闭。
    async with (
        meta_client_manager.session_factory() as meta_session,
        dw_client_manager.session_factory() as dw_session,
    ):
        # 基于 session 创建 MySQL Repository。
        meta_mysql_repository = MetaMySQLRepository(meta_session)
        dw_mysql_repository = DWMySQLRepository(dw_session)

        # 组装 DataAgentContext。
        #
        # 这里把图执行需要的依赖对象统一放进 context，
        # 图中各节点运行时可以通过 runtime.context 取到它们。
        context = DataAgentContext(
            metric_qdrant_repository=metric_qdrant_repository,
            value_es_repository=value_es_repository,
            column_qdrant_repository=column_qdrant_repository,
            embedding_client=embedding_client,
            meta_mysql_repository=meta_mysql_repository,
            dw_mysql_repository=dw_mysql_repository,
        )

        # 以流式方式执行整张图。
        #
        # 参数说明：
        # - input=state：初始状态
        # - context=context：运行时依赖上下文
        # - stream_mode="custom"：接收节点中通过 runtime.stream_writer 写出的自定义事件
        #
        # 每次迭代得到一个 chunk，并打印出来。
        async for chunk in graph.astream(
            input=state, context=context, stream_mode="custom"
        ):
            print(chunk)


if __name__ == "__main__":
    # 打印图的 Mermaid 描述。
    # 这样可以在本地快速查看工作流结构。
    print(graph.get_graph().draw_mermaid())

    # 如果你想直接运行异步主流程，
    # 通常还需要补一行：
    # asyncio.run(main())
    #
    # 当前这份代码只会打印 Mermaid 图，不会真正执行 main()。
