# Runtime：
# 来自 LangGraph 的运行时对象类型。
# 它通常包含：
# - context：运行时上下文依赖
# - stream_writer：流式输出函数
#
# 在这个节点里，会用到：
# - runtime.stream_writer：向外部输出当前阶段和执行结果
# - runtime.context["dw_mysql_repository"]：执行 SQL 的数据库仓储对象
from langgraph.runtime import Runtime

# DataAgentContext：
# 项目定义的图运行时上下文类型。
# 这个节点会从 runtime.context 中取出：
# - dw_mysql_repository
#
# 用来真正执行生成好的 SQL。
from app.agent.context import DataAgentContext

# DataAgentState：
# 图运行过程中流转的状态对象类型。
# 这个节点会从 state 中取出：
# - sql：前面 generate_sql / correct_sql 节点生成或修正后的 SQL
from app.agent.state import DataAgentState

# logger：
# 项目统一日志对象。
# 用于记录 SQL 执行成功或失败信息。
from app.core.logging import logger

# DWMySQLRepository：
# 面向数仓数据库（dw）的 MySQL 仓储对象。
# 这个对象封装了执行 SQL 的逻辑。
#
# 在这个节点里，会通过它调用 execute_sql(sql)，
# 去真正执行最终 SQL。
from app.repositories.mysql.dw_mysql_repository import DWMySQLRepository


async def execute_sql(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    """
    执行 SQL 节点。

    这个节点的核心作用是：
    1. 从 state 中取出最终 SQL
    2. 从 runtime.context 中取出 dw_mysql_repository
    3. 在 DW 数据库中执行 SQL
    4. 记录执行日志
    5. 通过 stream_writer 把执行结果流式输出

    为什么需要这个节点？
    因为前面的节点只是：
    - 召回语义信息
    - 筛选上下文
    - 生成 SQL
    - 校验或修正 SQL

    真正把用户问题转成“查询结果”的最后一步，
    就是在这里实际执行 SQL。

    也就是说，这个节点是整个问数链路的最终落地执行阶段。
    """

    # 从 runtime 中取出流式写入器。
    # 用于向外部实时反馈当前节点阶段和最终执行结果。
    writer = runtime.stream_writer

    # 输出当前阶段：正在执行 SQL 语句。
    writer({"stage": "执行SQL语句"})

    # 从 state 中取出前面节点已经准备好的 SQL。
    #
    # 这个 sql 一般来自：
    # - generate_sql 节点
    # 或
    # - correct_sql 节点
    sql = state["sql"]

    # 从 runtime.context 中取出 DW MySQL 仓储对象。
    #
    # 这个 repository 封装了对数仓数据库的访问能力，
    # 包括真正执行 SQL。
    dw_mysql_repository: DWMySQLRepository = runtime.context["dw_mysql_repository"]

    try:
        # 调用 DW 仓储执行 SQL。
        #
        # 这里的 execute_sql(sql) 会把最终 SQL 发到数仓数据库中执行，
        # 并返回查询结果。
        #
        # 返回结果的结构取决于你 DWMySQLRepository 的实现，
        # 可能是：
        # - list[dict]
        # - 标量
        # - 行对象列表
        # - 经过格式化后的 JSON 结果
        result = await dw_mysql_repository.execute_sql(sql)

        # 记录日志，方便调试时查看最终 SQL 执行结果。
        logger.info(f"SQL执行结果: {result}")

        # 通过 stream_writer 把查询结果流式输出。
        #
        # 这样前端或调用方在 graph.astream(stream_mode="custom") 时，
        # 就能接收到这条结果消息。
        writer({"result": result})

    except Exception as e:
        # 如果执行 SQL 失败，则记录错误日志。
        logger.error(f"SQL执行失败: {e}")

        # 把异常继续抛出，交由上层图执行流程处理。
        raise