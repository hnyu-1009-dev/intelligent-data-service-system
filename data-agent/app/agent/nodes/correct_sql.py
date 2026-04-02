# StrOutputParser：
# LangChain 提供的字符串输出解析器。
# 它的作用是把大模型输出解析成普通字符串。
#
# 在这段代码里，我们希望模型最终返回的是“修正后的 SQL 文本”，
# 所以这里使用 StrOutputParser，而不是 JsonOutputParser。
from langchain_core.output_parsers import StrOutputParser

# PromptTemplate：
# LangChain 提供的提示词模板类。
# 用于构造带变量占位符的 prompt。
#
# 在这里，会把：
# - query
# - sql
# - error
# - table_infos
# - metric_infos
# - date_info
# - db_info
# 等信息统一填入 prompt，让大模型基于错误信息修正 SQL。
from langchain_core.prompts import PromptTemplate

# Runtime：
# 来自 LangGraph 的运行时对象类型。
# 它通常包含：
# - context：运行时依赖对象
# - stream_writer：流式输出函数
from langgraph.runtime import Runtime

# DataAgentContext：
# 项目定义的图运行时上下文类型。
# 这个节点当前没有直接从 runtime.context 里取依赖，
# 但函数签名保持统一，方便整张图风格一致。
from app.agent.context import DataAgentContext

# llm：
# 项目统一封装好的大模型对象。
# 这里用它根据“原始 SQL + 错误信息 + 上下文”重新生成修正后的 SQL。
from app.agent.llm import llm

# DataAgentState：
# 图运行过程中在各节点之间流转的状态对象类型。
# 这个节点会从 state 中读取：
# - query：用户原始问题
# - sql：前一步生成的 SQL
# - error：SQL 校验或执行时报错信息
# - table_infos：表信息上下文
# - metric_infos：指标信息上下文
# - date_info：日期上下文
# - db_info：数据库上下文
from app.agent.state import DataAgentState

# logger：
# 项目统一日志对象。
from app.core.logging import logger

# load_prompt：
# 项目封装的 prompt 加载函数。
# 用于按名称读取 prompt 模板内容。
from app.prompt.prompt_loader import load_prompt


async def correct_sql(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    """
    校正 SQL 节点。

    这个节点的核心作用是：
    1. 读取前面生成的 SQL
    2. 读取 SQL 校验 / 执行阶段返回的错误信息
    3. 结合用户原始问题和完整上下文
    4. 让大模型生成“修正后的 SQL”
    5. 把修正后的 SQL 再写回 state["sql"]

    为什么需要这个节点？
    因为大模型一次性生成 SQL 不一定百分百正确，
    常见错误包括：
    - 表名写错
    - 字段名写错
    - join 条件不完整
    - 指标字段用错
    - 时间过滤条件有问题
    - SQL 语法错误

    所以这里通过“原始 SQL + error + 上下文”再做一次修正，
    相当于给模型一个自我纠错的机会。
    """

    # 从 runtime 中取出流式写入器。
    # 用于向外部实时汇报当前执行阶段。
    writer = runtime.stream_writer

    # 输出当前阶段：正在校正 SQL。
    writer({"stage": "校正SQL"})

    # 从 state 中取出用户原始问题。
    # 让模型在校正时不要脱离原始需求。
    query = state["query"]

    # 从 state 中取出上一轮生成的 SQL。
    sql = state["sql"]

    # 从 state 中取出错误信息。
    # 这个 error 一般来自：
    # - validate_sql 节点
    # 或
    # - execute_sql 节点
    #
    # 它会告诉模型：
    # 当前 SQL 到底哪里有问题。
    error = state["error"]

    # 从 state 中取出表信息上下文。
    # 里面一般包含：
    # - 相关表
    # - 表描述
    # - 当前问题相关字段
    table_infos = state["table_infos"]

    # 从 state 中取出指标信息上下文。
    metric_infos = state["metric_infos"]

    # 从 state 中取出时间上下文。
    date_info = state["date_info"]

    # 从 state 中取出数据库上下文。
    db_info = state["db_info"]

    try:
        # 构造 PromptTemplate。
        #
        # 这里加载名为 "correct_sql" 的提示词模板。
        # 模板会同时使用：
        # - query：用户问题
        # - sql：原始生成 SQL
        # - error：错误信息
        # - table_infos：表上下文
        # - metric_infos：指标上下文
        # - date_info：时间上下文
        # - db_info：数据库上下文
        #
        # 这样模型就能基于“问题 + 错误原因 + schema 信息”
        # 来重新修正 SQL。
        prompt = PromptTemplate(
            template=load_prompt("correct_sql"),
            input_variables=[
                "query",
                "sql",
                "error",
                "table_infos",
                "metric_infos",
                "date_info",
                "db_info",
            ],
        )

        # 创建字符串输出解析器。
        # 因为这里最终希望得到的是一段 SQL 文本。
        output_parser = StrOutputParser()

        # 拼接 LangChain 调用链：
        # prompt -> llm -> output_parser
        #
        # 含义是：
        # 1. 先组织 prompt
        # 2. 调用大模型
        # 3. 把结果解析成字符串
        chain = prompt | llm | output_parser

        # 异步调用整条链，生成“修正后的 SQL”。
        #
        # 模型此时拿到的信息非常完整：
        # - 原始 query：用户真正想问什么
        # - 原始 sql：模型上一次生成了什么
        # - error：为什么失败
        # - table_infos / metric_infos：有哪些表、字段、指标可以用
        # - date_info / db_info：额外上下文
        #
        # 例如：
        # 原始 SQL 里写错了字段名 order_amout
        # error 提示 Unknown column 'order_amout'
        # 那模型就有机会改成正确的 order_amount
        result = await chain.ainvoke(
            {
                "query": query,
                "sql": sql,
                "error": error,
                "table_infos": table_infos,
                "metric_infos": metric_infos,
                "date_info": date_info,
                "db_info": db_info,
            }
        )

        # 记录日志，方便观察修正后的 SQL。
        logger.info(f"校正SQL结果：{result}")

        # 返回给下游节点的状态增量。
        #
        # 这里把修正后的 SQL 重新写回 state["sql"]。
        # 后续节点（通常是再次校验或执行）会继续使用这个新 SQL。
        return {"sql": result}

    except Exception as e:
        # 如果校正过程失败，记录错误日志。
        logger.error(f"校正SQL失败：{e}")

        # 继续把异常抛给上层，交由图执行流程统一处理。
        raise