# JsonOutputParser：
# LangChain 提供的输出解析器。
# 它的作用是把大模型返回的内容解析成 JSON / Python 结构。
#
# 在这段代码里，它用于约束大模型输出：
# “哪些指标应该被保留”
#
# 这样后续代码就可以直接把模型结果当成 Python 列表或字典来处理。
from langchain_core.output_parsers import JsonOutputParser

# PromptTemplate：
# LangChain 提供的提示词模板类。
# 用来定义带变量占位符的 prompt。
#
# 这里会把：
# - query：用户原始问题
# - metric_infos：候选指标信息
# 作为变量填进 prompt 模板中，
# 让大模型根据问题语义判断哪些指标真正相关。
from langchain_core.prompts import PromptTemplate

# Runtime：
# 来自 LangGraph 的运行时对象类型。
# 它一般包含：
# - context：运行时依赖对象
# - stream_writer：用于流式输出当前阶段信息
from langgraph.runtime import Runtime

# DataAgentContext：
# 项目定义的图运行时上下文类型。
# 虽然这个节点当前没有显式从 runtime.context 中取依赖，
# 但函数签名仍然统一保持 (state, runtime) 风格，
# 这样所有节点接口一致，更容易维护。
from app.agent.context import DataAgentContext

# llm：
# 项目中统一封装好的大模型对象。
# 用来根据 prompt 执行推理并返回结果。
from app.agent.llm import llm

# DataAgentState：
# 图运行过程中流转的状态对象类型。
# 这个节点会从 state 中读取：
# - metric_infos：候选指标信息
# - query：用户原始问题
from app.agent.state import DataAgentState

# logger：
# 项目统一日志对象，用于记录节点执行结果和异常。
from app.core.logging import logger

# load_prompt：
# 项目封装的 prompt 加载函数。
# 一般用于按名称读取 prompt 模板内容。
from app.prompt.prompt_loader import load_prompt


async def filter_metric_info(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    """
    筛选指标信息节点。

    这个节点的核心作用是：
    1. 获取前面召回并整理好的候选指标信息 metric_infos
    2. 结合用户 query，让大模型判断哪些指标真正和当前问题相关
    3. 删除无关指标
    4. 返回筛选后的 metric_infos

    为什么需要这个节点？
    因为前面指标召回通常是“候选召回”，会尽量多召回一些可能相关的指标。
    为了减少噪音、提高后续 SQL 生成准确率，需要在这里做一次更精细的筛选。

    例如：
    用户问题是：
    “统计一下2025年1月份各品类的销售额占比”
    前面可能召回到：
    - GMV
    - AOV
    - 订单数
    但实际上真正相关的可能只有：
    - GMV
    所以这里需要进一步过滤。
    """

    # 从 runtime 中取出流式写入器。
    # 用于实时向外部报告当前节点所处阶段。
    writer = runtime.stream_writer

    # 向外部输出当前阶段：正在筛选指标信息。
    writer({"stage": "筛选指标信息"})

    # 从 state 中取出候选指标信息列表。
    #
    # metric_infos 一般来自前一个 merge_retrieved_info 节点，
    # 结构大概类似：
    # [
    #   {
    #     "name": "GMV",
    #     "description": "...",
    #     "alias": ["成交总额", "订单总额"]
    #   },
    #   {
    #     "name": "AOV",
    #     "description": "...",
    #     "alias": ["平均单价", "平均订单金额"]
    #   }
    # ]
    metric_infos = state["metric_infos"]

    # 从 state 中取出用户原始问题。
    query = state["query"]

    try:
        # 构造 prompt 模板。
        #
        # 这里加载名为 "filter_metric_info" 的提示词模板，
        # 并传入两个变量：
        # - query：用户问题
        # - metric_infos：候选指标信息
        #
        # 让大模型根据问题语义判断：
        # 哪些指标应该保留，哪些指标应该去掉。
        prompt = PromptTemplate(
            template=load_prompt("filter_metric_info"),
            input_variables=["query", "metric_infos"]
        )

        # 创建 JSON 输出解析器。
        # 这样大模型输出会被解析成 Python 结构，
        # 后续代码就能直接进行程序化筛选。
        output_parser = JsonOutputParser()

        # 使用 LangChain 的 LCEL 语法，把 prompt、llm、output_parser 串成一条链。
        #
        # 执行顺序是：
        # prompt -> llm -> output_parser
        chain = prompt | llm | output_parser

        # 异步调用整条链。
        #
        # 假设 query 是：
        # “统计一下2025年1月份各品类的销售额占比”
        #
        # metric_infos 是：
        # [
        #   {"name": "GMV", ...},
        #   {"name": "AOV", ...}
        # ]
        #
        # 大模型可能返回：
        # ["GMV"]
        #
        # 表示：只有 GMV 和当前问题真正相关，应当保留。
        result = await chain.ainvoke({"query": query, "metric_infos": metric_infos})

        # 遍历当前候选指标列表，并按模型返回结果做原地筛选。
        #
        # 注意这里使用 metric_infos[:] 做“浅拷贝遍历”，
        # 是因为后面会 remove 元素。
        # 如果直接遍历原列表，边遍历边删除可能会导致跳元素或行为异常。
        for metric_info in metric_infos[:]:
            # 如果当前指标名字不在大模型返回结果中，
            # 说明该指标与当前问题无关，应当从候选集合中移除。
            if metric_info["name"] not in result:
                metric_infos.remove(metric_info)

        # 记录筛选后的指标名称列表，方便日志观察。
        logger.info(f"指标筛选结果: {[metric_info['name'] for metric_info in metric_infos]}")

        # 返回给下游节点的状态增量。
        #
        # LangGraph 会把这个字典自动合并回全局 state，
        # 后续节点会继续使用更新后的 metric_infos。
        return {"metric_infos": metric_infos}

    except Exception as e:
        # 如果指标筛选过程失败，记录错误日志。
        logger.error(f"指标筛选失败: {str(e)}")

        # 继续把异常抛给上层，交由图执行流程统一处理。
        raise