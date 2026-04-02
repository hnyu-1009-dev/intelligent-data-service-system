# JsonOutputParser：
# LangChain 提供的输出解析器。
# 它的作用是把大模型返回的内容解析成 JSON / Python 结构。
#
# 在这段代码里，它用于约束大模型返回：
# “应该保留哪些表、每张表应该保留哪些字段”
#
# 这样后续代码就可以直接把模型输出当成 Python 字典来处理。
from langchain_core.output_parsers import JsonOutputParser

# PromptTemplate：
# LangChain 提供的提示词模板类。
# 用来定义带变量占位符的 prompt。
#
# 例如这里会把：
# - query
# - table_infos
# 作为变量填进 prompt 模板中，
# 让大模型根据用户问题和召回到的表信息做筛选。
from langchain_core.prompts import PromptTemplate

# Runtime：
# 来自 LangGraph 的运行时对象类型。
# 它一般包含：
# - context：运行时依赖对象
# - stream_writer：用于流式输出阶段信息
from langgraph.runtime import Runtime

# DataAgentContext：
# 项目定义的图运行时上下文类型。
# 虽然这个节点当前没有显式从 runtime.context 中取依赖，
# 但函数签名仍然保持一致，便于整张图风格统一。
from app.agent.context import DataAgentContext

# llm：
# 项目中统一封装好的大模型对象。
# 用于根据 prompt 执行推理并返回结果。
from app.agent.llm import llm

# DataAgentState：
# 图运行过程中流转的状态对象类型。
# 这个节点会从 state 中读取：
# - table_infos：前面 merge_retrieved_info 合并后的表信息
# - query：用户原始问题
from app.agent.state import DataAgentState

# logger：
# 项目统一日志对象。
from app.core.logging import logger

# load_prompt：
# 项目封装的 prompt 加载函数。
# 用于按名称读取 prompt 模板内容。
from app.prompt.prompt_loader import load_prompt


async def filter_table_info(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    """
    筛选表信息节点。

    这个节点的核心作用是：
    1. 拿到前面合并后的 table_infos
    2. 结合用户 query，让大模型判断：
       - 哪些表真正和当前问题相关
       - 每张表里哪些字段真正应该保留
    3. 删除无关表
    4. 删除表中无关字段
    5. 返回精简后的 table_infos

    为什么要做这一步？
    因为前面“召回 + 合并”得到的表和字段往往是候选集合，
    为了提高后续 SQL 生成准确率，需要进一步缩小上下文范围，
    避免把过多无关字段喂给大模型。
    """

    # 从 runtime 中取出流式写入器。
    # 用于向外部实时反馈当前节点执行到了哪个阶段。
    writer = runtime.stream_writer

    # 报告当前阶段：正在筛选表信息。
    writer({"stage": "筛选表信息"})

    # 从 state 中取出表信息集合。
    #
    # table_infos 一般来自前一个 merge_retrieved_info 节点，
    # 结构大概是：
    # [
    #   {
    #     "name": "fact_order",
    #     "role": "fact",
    #     "description": "...",
    #     "columns": [...]
    #   },
    #   {
    #     "name": "dim_region",
    #     ...
    #   }
    # ]
    table_infos = state["table_infos"]

    # 从 state 中取出用户原始问题。
    query = state["query"]

    try:
        # 构造 prompt 模板。
        #
        # 这里加载名为 "filter_table_info" 的提示词模板，
        # 并传入两个变量：
        # - query：用户问题
        # - table_infos：候选表信息
        #
        # 让大模型根据问题语义判断哪些表和字段应该保留。
        prompt = PromptTemplate(
            template=load_prompt("filter_table_info"),
            input_variables=["query", "table_infos"]
        )

        # 创建 JSON 输出解析器。
        # 这样大模型输出会被解析成 Python 字典 / 列表结构，
        # 便于后续直接做程序化筛选。
        output_parser = JsonOutputParser()

        # 拼接 LangChain 调用链：
        # prompt -> llm -> output_parser
        #
        # 含义是：
        # 1. 用 prompt 组织输入
        # 2. 调用 llm 进行推理
        # 3. 把 llm 输出解析成 JSON 结构
        chain = prompt | llm | output_parser

        # 异步调用链。
        #
        # 假设 query 是：
        # “统计一下2025年1月份各品类的销售额占比”
        #
        # table_infos 包含候选表：
        # - fact_order
        # - dim_product
        # - dim_date
        # - dim_region
        #
        # 那么大模型可能返回类似：
        # {
        #   "fact_order": ["order_amount", "date_id", "product_id"],
        #   "dim_product": ["category", "product_id"],
        #   "dim_date": ["month", "date_id"]
        # }
        #
        # 表示：
        # - 保留 fact_order、dim_product、dim_date
        # - 不保留 dim_region
        # - 每张表里只保留指定字段
        result = await chain.ainvoke({"query": query, "table_infos": table_infos})

        # 遍历当前的 table_infos，对表和字段进行原地筛选。
        #
        # 注意这里用了 table_infos[:]，这是“浅拷贝遍历”。
        # 原因是：后面会在遍历过程中 remove 元素，
        # 如果直接遍历原列表，可能会跳元素或行为异常。
        for table_info in table_infos[:]:
            # 如果当前表名不在大模型返回结果里，
            # 说明这个表与当前问题无关，应当移除。
            if table_info["name"] not in result:
                table_infos.remove(table_info)
            else:
                # 如果表被保留，则继续筛选这张表内部的字段
                #
                # 同样使用 table_info["columns"][:] 做浅拷贝遍历，
                # 避免遍历时 remove 导致的问题。
                for column in table_info["columns"][:]:
                    # 如果字段名不在当前表对应的保留字段列表中，
                    # 说明该字段与问题无关，应当移除。
                    if column["name"] not in result[table_info["name"]]:
                        table_info["columns"].remove(column)

        # 记录筛选后的表名列表，方便日志观察。
        logger.info(f"表格筛选结果: {[table_info['name'] for table_info in table_infos]}")

        # 返回给下游节点的状态增量。
        #
        # LangGraph 会把这个字典自动合并回全局 state，
        # 后续节点会继续使用更新后的 table_infos。
        return {"table_infos": table_infos}

    except Exception as e:
        # 如果筛选失败，记录错误日志。
        logger.error(f"表格筛选失败: {str(e)}")

        # 将异常继续抛出，交给上层流程处理。
        raise