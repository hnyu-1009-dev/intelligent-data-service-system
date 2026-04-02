# yaml：
# Python 中常用的 YAML 处理库。
# 这段代码里实际上没有使用 yaml，
# 所以严格来说它是一个“未使用导入”，如果不需要可以删除。
import yaml

# StrOutputParser：
# LangChain 提供的字符串输出解析器。
# 它的作用是把大模型最终输出解析成普通字符串。
#
# 在这段代码里，我们希望大模型最终返回的是一段 SQL 文本，
# 而不是 JSON 结构，所以这里使用 StrOutputParser。
from langchain_core.output_parsers import StrOutputParser

# PromptTemplate：
# LangChain 提供的提示词模板类。
# 用来定义带变量占位符的 prompt。
#
# 在这段代码里，会把：
# - table_infos
# - metric_infos
# - date_info
# - db_info
# - query
# 这些变量填充到提示词模板中，
# 让大模型基于完整上下文生成 SQL。
from langchain_core.prompts import PromptTemplate

# Runtime：
# 来自 LangGraph 的运行时对象类型。
# 它通常包含：
# - context：节点运行时可访问的依赖对象
# - stream_writer：用于流式输出当前阶段信息
from langgraph.runtime import Runtime

# DataAgentContext：
# 项目定义的图运行时上下文类型。
# 虽然这个节点当前没有显式从 runtime.context 中取依赖，
# 但函数签名保持统一，方便整张图中所有节点接口风格一致。
from app.agent.context import DataAgentContext

# llm：
# 项目中统一封装好的大模型对象。
# 这里会利用它根据 prompt 和上下文生成 SQL。
from app.agent.llm import llm

# DataAgentState：
# 图运行过程中流转的全局状态对象类型。
#
# 这个节点会从 state 中读取多个前序节点已经准备好的信息：
# - table_infos：筛选后的表信息
# - metric_infos：筛选后的指标信息
# - date_info：日期上下文
# - db_info：数据库上下文
# - query：用户原始问题
from app.agent.state import DataAgentState

# logger：
# 项目统一日志对象，用于记录 SQL 生成结果与异常信息。
from app.core.logging import logger

# load_prompt：
# 项目封装的 prompt 加载函数。
# 一般用于按名称读取 prompt 模板内容。
from app.prompt.prompt_loader import load_prompt


async def generate_sql(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    """
    生成 SQL 节点。

    这个节点的核心作用是：
    1. 从 state 中取出已经准备好的上下文信息
    2. 组合成一个完整 prompt
    3. 调用大模型生成 SQL
    4. 把生成结果写回 state["sql"]

    这个节点依赖的输入包括：
    - table_infos：候选表及字段信息
    - metric_infos：候选指标信息
    - date_info：日期上下文（当前日期、星期、季度）
    - db_info：数据库上下文
    - query：用户原始问题

    为什么这些输入很重要？
    因为大模型生成 SQL 时，不只是依赖用户原始 query，
    还需要知道：
    - 可以用哪些表
    - 可以用哪些字段
    - 有哪些业务指标
    - 当前日期和季度是什么
    - 当前数据库的结构信息

    这样才能更稳定、更准确地生成 SQL。
    """

    # 从 runtime 中取出流式写入器。
    # 用于向外部实时汇报当前节点执行阶段。
    writer = runtime.stream_writer

    # 输出当前阶段：正在生成 SQL。
    writer({"stage": "生成SQL"})

    # 从 state 中取出前序节点已经准备好的表信息。
    #
    # table_infos 通常来自：
    # merge_retrieved_info -> filter_table_info
    #
    # 它一般包含：
    # - 表名
    # - 表角色
    # - 表描述
    # - 该表最终保留的字段信息
    table_infos = state["table_infos"]

    # 从 state 中取出筛选后的指标信息。
    #
    # metric_infos 通常来自：
    # merge_retrieved_info -> filter_metric_info
    #
    # 它一般包含：
    # - 指标名
    # - 指标描述
    # - 指标别名
    metric_infos = state["metric_infos"]

    # 从 state 中取出日期上下文。
    #
    # date_info 通常来自 add_context 节点，
    # 包括：
    # - 当前日期
    # - 当前星期几
    # - 当前季度
    #
    # 这些信息可以帮助大模型处理相对时间表达，
    # 例如：
    # - 本月
    # - 本季度
    # - 今天
    date_info = state["date_info"]

    # 从 state 中取出数据库上下文。
    #
    # db_info 也通常来自 add_context 节点，
    # 用来向模型补充数据库结构相关信息。
    db_info = state["db_info"]

    # 从 state 中取出用户原始问题。
    query = state["query"]

    try:
        # 构造 PromptTemplate。
        #
        # 这里加载名为 "generate_sql" 的提示词模板。
        #
        # input_variables 指定了模板中会用到的变量：
        # - table_infos
        # - metric_infos
        # - date_info
        # - db_info
        #
        # 注意：
        # 这里虽然 input_variables 中没有显式写 query，
        # 但后面 chain.ainvoke() 仍然传入了 query。
        # 这通常意味着：
        # - 要么 prompt 模板里实际上也用了 query，但这里漏写了
        # - 要么当前模板不使用 query，这里多传了一个变量
        #
        # 从业务角度看，generate_sql 几乎一定应该使用 query，
        # 所以这里更合理的写法通常应把 "query" 也放进 input_variables。
        prompt = PromptTemplate(
            template=load_prompt("generate_sql"),
            input_variables=["table_infos", "metric_infos", "date_info", "db_info"]
        )

        # 创建字符串输出解析器。
        #
        # 因为这里期望模型最终输出的是一段 SQL 文本，
        # 所以使用 StrOutputParser，把模型输出解析成 str。
        output_parser = StrOutputParser()

        # 使用 LangChain 的 LCEL 语法把：
        # prompt -> llm -> output_parser
        # 串成一条链。
        #
        # 含义是：
        # 1. 用 prompt 组织完整输入
        # 2. 调用 llm 生成结果
        # 3. 把结果解析成字符串
        chain = prompt | llm | output_parser

        # 异步调用整条链，生成 SQL。
        #
        # 这里传给大模型的上下文很完整，包括：
        # - table_infos：有哪些表和字段可用
        # - metric_infos：有哪些指标可用
        # - date_info：当前日期上下文
        # - db_info：数据库上下文
        # - query：用户原始问题
        #
        # 例如用户问题：
        # “统计一下2025年1月份各品类的销售额占比”
        #
        # 模型可能生成：
        # SELECT
        #   p.category,
        #   SUM(o.order_amount) / SUM(SUM(o.order_amount)) OVER() AS amount_ratio
        # FROM fact_order o
        # JOIN dim_product p ON o.product_id = p.product_id
        # JOIN dim_date d ON o.date_id = d.date_id
        # WHERE d.year = 2025 AND d.month = 1
        # GROUP BY p.category;
        result = await chain.ainvoke(
            {
                "table_infos": table_infos,
                "metric_infos": metric_infos,
                "date_info": date_info,
                "db_info": db_info,
                "query": query,
            }
        )

        # 记录日志，方便观察模型最终生成的 SQL。
        logger.info(f"生成SQL: {result}")

        # 返回给下游节点的状态增量。
        #
        # LangGraph 会自动把这个结果合并回全局 state，
        # 后续 validate_sql 节点可以从 state["sql"] 中读取它。
        return {"sql": result}

    except Exception as e:
        # 如果 SQL 生成失败，记录错误日志。
        logger.error(f"生成SQL失败: {e}")

        # 继续把异常抛给上层流程处理。
        raise