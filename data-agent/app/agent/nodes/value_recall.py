# JsonOutputParser：
# LangChain 提供的输出解析器。
# 作用是把大模型返回的内容解析成 JSON / Python 结构。
#
# 在这里，它用于约束大模型输出“扩展后的值召回关键词列表”，
# 这样后续代码就能直接把结果当成 Python 列表来使用。
from langchain_core.output_parsers import JsonOutputParser

# PromptTemplate：
# LangChain 提供的提示词模板类。
# 用来定义带变量占位符的 prompt。
#
# 例如：
# template = "请根据用户问题补充一些适合字段值召回的关键词：{query}"
#
# 调用时再传入 query 的实际内容。
from langchain_core.prompts import PromptTemplate

# Runtime：
# 来自 langgraph 的运行时对象类型。
# 它通常包含：
# - context：当前节点执行时可访问的上下文依赖
# - stream_writer：流式输出函数
from langgraph.runtime import Runtime

# DataAgentContext：
# 项目定义的图运行时上下文类型。
# 一般会包含当前节点执行需要的依赖对象，比如：
# - value_es_repository
from app.agent.context import DataAgentContext

# llm：
# 项目中统一封装好的大模型对象。
# 用于根据 prompt 生成结果。
from app.agent.llm import llm

# DataAgentState：
# 图运行过程中在各节点之间流转的状态对象类型。
# 这里会从 state 中读取：
# - keywords：前序节点提取出的关键词
# - query：用户原始问题
from app.agent.state import DataAgentState

# logger：
# 项目统一日志对象。
from app.core.logging import logger

# ValueInfoES：
# 项目定义的 Elasticsearch 值文档模型。
# 一条值文档通常会包含：
# - id
# - value
# - type
# - column_id
# - column_name
# - table_id
# - table_name
#
# 这里把它用作类型标注，表示 ES 召回出的结果结构。
from app.models.es.value_info_es import ValueInfoES

# load_prompt：
# 项目封装的 prompt 加载函数。
# 一般用于从 prompt 文件中按名称加载提示词模板内容。
from app.prompt.prompt_loader import load_prompt

# ValueESRepository：
# 项目中封装好的 ES 仓储对象。
# 用于对 Elasticsearch 做值检索。
#
# 这里会调用它的 query() 方法，根据关键词去 ES 中召回字段值。
from app.repositories.es.value_es_repository import ValueESRepository


async def value_recall(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    """
    召回字段值。

    这个节点的核心作用是：
    1. 获取用户原始问题和已有关键词
    2. 让大模型根据 query 扩展更多适合“值检索”的关键词
    3. 用这些关键词去 Elasticsearch 中检索具体字段值
    4. 对召回结果去重
    5. 把最终字段值召回结果写回 state，供后续节点继续使用

    为什么需要这个节点？
    因为用户问题中经常会提到“具体业务值”，而不是字段名。
    例如：
    - 华东
    - 黄金会员
    - 苹果
    - 上海

    这些值通常不是 schema 字段本身，而是字段中的真实取值。
    所以这里通过 ES 做值召回，帮助系统识别：
    “用户问题里提到的这个词，属于哪个字段的值”
    """

    # 从 runtime 中取出流式写入器。
    # 用于实时向外部反馈当前节点执行阶段。
    writer = runtime.stream_writer

    # 向外部报告当前节点阶段：正在召回字段值。
    writer({"stage": "召回字段值"})

    # 从 state 中取出已有关键词。
    # 这些关键词一般来自上游节点，例如 extract_keywords。
    keywords = state["keywords"]

    # 从 state 中取出用户原始问题。
    query = state["query"]

    # 从 runtime.context 中取出 Elasticsearch 仓储对象。
    # 后续会通过它到 ES 中检索具体字段值。
    value_es_repository: ValueESRepository = runtime.context["value_es_repository"]

    try:
        # 构造 prompt 模板。
        #
        # 这里加载名为 "extend_keywords_for_value_recall" 的提示词模板，
        # 目的是让大模型根据用户 query 扩展更多适合“字段值召回”的关键词。
        #
        # 例如：
        # query = "统计一下华东地区黄金会员的销售额"
        # 扩展后可能得到：
        # ["华东", "地区", "黄金会员", "会员等级"]
        prompt = PromptTemplate(
            template=load_prompt("extend_keywords_for_value_recall"),
            input_variables=["query"]
        )

        # 创建 JSON 输出解析器。
        # 确保大模型返回的扩展关键词结果能被解析成 Python 列表等结构，
        # 而不是一段自由文本。
        output_parser = JsonOutputParser()

        # 使用 LCEL 语法把 prompt、llm、output_parser 串成一条链。
        #
        # 整体执行过程是：
        # prompt -> llm -> output_parser
        chain = prompt | llm | output_parser

        # 异步调用整条链。
        #
        # 输入用户 query，让大模型返回一组扩展后的值召回关键词。
        result = await chain.ainvoke(input={"query": query})

        # 把原始关键词和大模型扩展出来的关键词合并，再去重。
        #
        # 这样可以：
        # 1. 保留已有关键词
        # 2. 增加更适合 ES 值召回的关键词
        # 3. 避免重复检索相同内容
        keywords = list(set(keywords + result))

        # 用于保存最终召回到的值文档。
        #
        # 为什么用 dict？
        # 因为同一个值可能被多个关键词重复召回，
        # 所以这里用 value["id"] 作为 key 做去重。
        values_map: dict[str, ValueInfoES] = {}

        # 遍历每个关键词，逐个到 ES 中做值检索。
        for keyword in keywords:
            # 用当前关键词在 Elasticsearch 中查询字段值。
            #
            # 参数说明：
            # - keyword：当前检索关键词
            # - score_threshold=0.6：相似度 / 相关度阈值
            # - limit=5：最多返回 5 条结果
            #
            # 返回的 values 一般是值文档列表，
            # 每个元素可能类似：
            # {
            #   "id": "dim_region.region_name.华东",
            #   "value": "华东",
            #   "column_id": "dim_region.region_name",
            #   "column_name": "region_name",
            #   "table_id": "dim_region",
            #   "table_name": "dim_region"
            # }
            values = await value_es_repository.query(keyword, score_threshold=0.6, limit=5)

            # 把本次关键词召回到的值文档加入去重 map
            for value in values:
                if value["id"] not in values_map:
                    values_map[value["id"]] = value

        # 取出去重后的所有值召回结果。
        #
        # values_map.values() 返回的是 dict_values 视图对象，
        # 里面只包含 value，不包含 key。
        retrieved_values = values_map.values()

        # 记录日志，方便调试和观察当前召回到的值文档 id。
        logger.info(f"召回字段值: {values_map.keys()}")

        # 返回给下游节点的状态增量。
        #
        # LangGraph 会把这个字典自动合并回全局 state，
        # 后续节点可以通过 state["retrieved_values"] 继续使用。
        return {"retrieved_values": retrieved_values}

    except Exception as e:
        # 如果值召回过程失败，记录错误日志。
        logger.error(f"召回字段值失败: {str(e)}")

        # 继续把异常抛给上层，交由图或调用方处理。
        raise