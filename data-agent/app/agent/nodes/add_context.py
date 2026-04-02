# datetime：
# Python 标准库 datetime 模块中的 datetime 类。
# 用于获取当前时间、格式化日期等。
#
# 在这段代码里，它主要用于：
# - 获取今天的日期
# - 计算今天属于哪个季度
# - 构造 date_info 上下文信息
from datetime import datetime

# Runtime：
# 来自 LangGraph 的运行时对象类型。
# 它通常包含：
# - context：运行时上下文依赖
# - stream_writer：流式输出函数
from langgraph.runtime import Runtime

# DataAgentContext：
# 项目定义的图运行时上下文类型。
# 这个节点会从 runtime.context 中取出：
# - dw_mysql_repository
#
# 用于查询当前数据库的相关信息。
from app.agent.context import DataAgentContext

# DataAgentState：
# 图运行过程中流转的全局状态对象类型。
#
# DateInfoState：
# 用于保存“当前日期上下文”的状态对象。
#
# DBInfoState：
# 用于保存“数据库信息上下文”的状态对象。
from app.agent.state import DataAgentState, DateInfoState, DBInfoState

# logger：
# 项目统一日志对象。
from app.core.logging import logger


async def add_context(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    """
    添加上下文信息节点。

    这个节点的核心作用是：
    1. 生成当前时间上下文（date_info）
    2. 获取数据库信息上下文（db_info）
    3. 把这两部分上下文写回 state
    4. 供后续 SQL 生成节点使用

    为什么需要这个节点？
    因为用户问题中经常会涉及：
    - 当前日期
    - 当前季度
    - 今天是星期几
    - 当前数据库里有哪些表、字段或结构信息

    这些信息虽然不是从用户 query 中直接提取出来的，
    但会影响大模型后续生成 SQL 的准确性。

    所以这里会统一补充：
    - 时间上下文
    - 数据库上下文
    """

    # 从 runtime 中取出流式写入器。
    # 用于向外部实时汇报当前节点执行阶段。
    writer = runtime.stream_writer

    # 输出当前阶段：正在添加上下文信息。
    writer({"stage": "添加上下文信息"})

    # 获取当前系统时间。
    #
    # 例如：
    # 2026-04-01 14:30:00
    today = datetime.today()

    # 根据当前月份计算当前季度。
    #
    # 计算逻辑：
    # month = 1,2,3   -> Q1
    # month = 4,5,6   -> Q2
    # month = 7,8,9   -> Q3
    # month = 10,11,12 -> Q4
    #
    # 公式解释：
    # (today.month - 1) // 3 + 1
    # 可以把月份映射成季度编号
    quarter = f"Q{(today.month - 1) // 3 + 1}"

    # 构造日期上下文对象。
    #
    # 包含：
    # - date：当前日期，格式 YYYY-MM-DD
    # - weekday：当前星期几，例如 Monday / Tuesday
    # - quarter：当前季度，例如 Q1 / Q2
    #
    # 这些信息会在后续节点中作为“时间上下文”提供给大模型。
    date_info = DateInfoState(
        date=today.strftime("%Y-%m-%d"),
        weekday=today.strftime("%A"),
        quarter=quarter,
    )

    # 从 runtime.context 中取出 dw_mysql_repository，
    # 调用其 get_db_info() 方法获取数据库信息。
    #
    # 这里的 get_db_info() 一般会返回一个字典，
    # 例如：
    # {
    #   "database_name": "dw",
    #   "tables": [...],
    #   ...
    # }
    #
    # 然后用 ** 解包传给 DBInfoState，
    # 构造成结构化的数据库信息对象。
    db_info = DBInfoState(
        **await runtime.context["dw_mysql_repository"].get_db_info()
    )

    # 记录日志，方便调试时查看当前注入的上下文信息。
    logger.info(f"添加上下文信息-date_info:{date_info},db_info:{db_info}")

    # 返回给下游节点的状态增量。
    #
    # LangGraph 会自动把返回值合并回全局 state，
    # 后续节点（如 generate_sql）就可以直接从 state 中读取：
    # - state["date_info"]
    # - state["db_info"]
    return {
        "date_info": date_info,
        "db_info": db_info,
    }