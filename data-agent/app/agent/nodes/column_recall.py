# JsonOutputParser：
# LangChain 提供的输出解析器。
# 它的作用是把大模型返回的内容解析成 JSON / Python 结构。
#
# 在这段代码里，它用于约束大模型输出“扩展关键词列表”，
# 这样后续代码就可以直接把结果当成 Python 列表来处理。
from langchain_core.output_parsers import JsonOutputParser

# PromptTemplate：
# LangChain 提供的提示词模板类。
# 用来定义带变量占位符的 prompt。
#
# 例如：
# template = "请根据用户问题补充关键词：{query}"
#
# 后续调用时，只需要传入 query 的实际值即可。
from langchain_core.prompts import PromptTemplate

# HuggingFaceEndpointEmbeddings：
# LangChain 提供的 embedding 客户端封装类。
# 在本项目中，它负责调用远程 embedding 服务，
# 把关键词转换成向量，用于在 Qdrant 中做相似度检索。
from langchain_huggingface import HuggingFaceEndpointEmbeddings

# Runtime：
# 来自 langgraph 的运行时对象类型。
# 运行时中通常会携带：
# - context：上下文依赖对象（如 repository、client）
# - stream_writer：用于流式输出中间阶段信息
from langgraph.runtime import Runtime

# DataAgentContext：
# 项目定义的 Agent 上下文类型。
# 一般用于描述运行时 context 中会放哪些依赖对象，
# 例如：
# - column_qdrant_repository
# - embedding_client
from app.agent.context import DataAgentContext

# llm：
# 项目中统一封装好的大模型对象。
# 它负责接收 prompt 并返回模型生成结果。
from app.agent.llm import llm

# DataAgentState：
# 项目定义的 Agent 状态类型。
# 它通常表示当前节点执行时可读写的状态数据，
# 例如：
# - query：用户原始问题
# - keywords：前序节点提取出的关键词
# - retrieved_columns：本节点召回出的字段信息
from app.agent.state import DataAgentState

# logger：
# 项目统一日志对象，用于记录节点执行成功 / 失败信息。
from app.core.logging import logger

# ColumnInfoQdrant：
# 项目定义的字段向量检索结果模型。
# 它表示从 Qdrant 中召回出来的一条字段信息 payload。
from app.models.qdrant.column_info_qdrant import ColumnInfoQdrant

# load_prompt：
# 项目封装的 prompt 加载函数。
# 一般用于从 prompt 文件中按名称加载提示词模板内容。
from app.prompt.prompt_loader import load_prompt

# ColumnQdrantRepository：
# 面向 Qdrant 的字段仓储对象。
# 提供字段向量检索能力，例如：
# - search(embedding, threshold, limit)
from app.repositories.qdrant.column_repository_qdrant import ColumnQdrantRepository


async def column_recall(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    """
    从向量数据库中召回列信息。

    这个节点的核心目标是：
    1. 获取用户问题和已有关键词
    2. 让大模型基于 query 扩展更多适合字段召回的关键词
    3. 使用 embedding 模型把每个关键词向量化
    4. 到 Qdrant 中检索相似字段
    5. 对检索结果去重
    6. 返回最终召回到的字段信息列表

    为什么要这样做？
    因为用户问题中的表达不一定直接等于数据库字段名。
    例如：
    - “销售额” 可能对应 fact_order.order_amount
    - “会员等级” 可能对应 dim_customer.member_level

    所以这里采用：
    - LLM 做关键词扩展
    - Embedding + Qdrant 做语义召回
    的方式，提高字段识别准确率。
    """

    # 从 runtime 中获取流式写入器。
    # 这个 writer 一般用于把当前节点执行到哪个阶段，实时反馈给前端或调用方。
    writer = runtime.stream_writer

    # 向外部报告当前阶段：
    # 说明当前节点正在执行“字段信息召回”。
    writer({"stage": "召回字段信息"})

    # 从状态中取出已有关键词。
    # 这些关键词通常来自上游节点，比如：
    # - 用户问题分词
    # - LLM 提取关键词
    keywords = state["keywords"]

    # 从状态中取出用户原始查询。
    # 例如：
    # “华东地区黄金会员的销售额是多少”
    query = state["query"]

    # 从运行时上下文中取出字段 Qdrant 仓储对象。
    # 后续会通过它到 Qdrant 中检索字段语义信息。
    column_qdrant_repository: ColumnQdrantRepository = runtime.context[
        "column_qdrant_repository"
    ]

    # 从运行时上下文中取出 embedding 客户端。
    # 后续会把关键词转换为向量。
    embedding_client: HuggingFaceEndpointEmbeddings = runtime.context[
        "embedding_client"
    ]

    try:
        # 构造 prompt 模板。
        #
        # 这里加载名为 "extend_keywords_for_column_recall" 的提示词模板，
        # 用于让大模型根据用户 query 扩展更多适合“字段召回”的关键词。
        #
        # input_variables=["query"] 表示这个 prompt 模板里会使用变量 {query}。
        prompt = PromptTemplate(
            template=load_prompt("extend_keywords_for_column_recall"),
            input_variables=["query"],
        )

        # 创建 JSON 输出解析器。
        # 目的是确保大模型返回的扩展关键词结果能被解析成 Python 结构，
        # 而不是一段自由文本。
        output_parser = JsonOutputParser()

        # 通过 LangChain 的 LCEL 语法拼接链：
        # prompt -> llm -> output_parser
        #
        # 含义是：
        # 1. 用 prompt 组织输入
        # 2. 调用 llm 生成结果
        # 3. 用 output_parser 把结果解析成 JSON / Python 对象
        chain = prompt | llm | output_parser

        # 异步调用整条链。
        #
        # 传入 {"query": query} 后，
        # 大模型会根据 query 返回一组扩展关键词。
        #
        # 例如：
        # 原始 query = "华东地区黄金会员的销售额是多少"
        # result 可能返回：
        # ["销售额", "订单金额", "收入", "地区", "会员等级"]
        result = await chain.ainvoke({"query": query})

        # 将原始关键词和大模型扩展出的关键词合并，并使用 set 去重。
        #
        # 这里的目的：
        # 1. 保留上游节点已有关键词
        # 2. 融合 LLM 补充的语义相关词
        # 3. 避免重复检索相同关键词
        keywords = list(set(keywords + result))

        # 用于保存最终召回到的字段信息。
        #
        # 为什么使用 dict？
        # 因为一个字段可能会被多个关键词同时召回，
        # 所以这里使用字段 id 作为 key 去重。
        #
        # 例如：
        # “销售额”和“订单金额”都可能召回 fact_order.order_amount
        columns_map: dict[str, ColumnInfoQdrant] = {}

        # 遍历每个关键词，逐个做向量检索
        for keyword in keywords:
            # 先把当前关键词转换成向量
            #
            # 例如：
            # keyword = "销售额"
            # embedding = [0.12, -0.07, ...]
            embedding = await embedding_client.aembed_query(keyword)

            # 使用向量到 Qdrant 中搜索相似字段
            #
            # 参数说明：
            # - embedding：查询向量
            # - 0.6：相似度阈值
            # - 5：最多返回 5 条结果
            #
            # 返回值通常是字段 payload 列表。
            columns: list[ColumnInfoQdrant] = await column_qdrant_repository.search(
                embedding, 0.6, 5
            )

            # 遍历当前关键词召回到的字段
            for column in columns:
                # 如果这个字段 id 还没有出现过，则加入结果字典
                #
                # 这样可以避免同一个字段被多个关键词重复加入。
                if column["id"] not in columns_map:
                    columns_map[column["id"]] = column

        # 取出最终去重后的字段信息集合，但是retrieved_columns是一个字典的视图而不是一个list
        retrieved_columns = columns_map.values()

        # 记录召回成功日志
        # 这里打印的是召回到的字段 id 集合
        logger.info(f"字段信息召回成功: {columns_map.keys()}")

        # 返回给下游节点的状态增量
        #
        # 这里会把召回到的字段信息列表写回状态，
        # 供后续节点继续使用，比如：
        # - 表过滤
        # - SQL 生成
        # - 上下文补充
        return {"retrieved_columns": list(retrieved_columns)}

    except Exception as e:
        # 如果字段召回过程出错，记录错误日志
        logger.error(f"字段信息召回失败: {str(e)}")

        # 抛出异常，交给上层流程处理
        raise
