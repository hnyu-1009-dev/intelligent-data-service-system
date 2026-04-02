# yaml：
# Python 中常用的 YAML 处理库。
# 这段代码里实际上没有使用 yaml，
# 所以严格来说这是一个“未使用导入”，如果不需要可以删除。
import yaml

# Runtime：
# 来自 langgraph 的运行时对象类型。
# 它通常包含：
# - context：运行时依赖对象
# - stream_writer：流式输出函数
from langgraph.runtime import Runtime

# DataAgentContext：
# 项目定义的运行时上下文类型。
# 一般用于声明 graph 节点执行时可以从 runtime.context 中拿到哪些依赖对象，
# 例如：
# - meta_mysql_repository
from app.agent.context import DataAgentContext

# DataAgentState：
# 图运行过程中的全局状态类型。
# TableInfoState / ColumnInfoState / MetricInfoState：
# 这些是项目中定义的“状态对象类型”，用于在 graph 节点之间传递结构化信息。
from app.agent.state import (
    DataAgentState,
    TableInfoState,
    ColumnInfoState,
    MetricInfoState,
)

# logger：
# 项目统一日志对象。
from app.core.logging import logger

# MySQL 中的字段、表元信息模型。
# 它们一般对应 meta 数据库中的表结构。
from app.models.mysql.column_info_mysql import ColumnInfoMySQL
from app.models.mysql.table_info_mysql import TableInfoMySQL

# Qdrant 中字段 payload 的数据模型。
# 它表示从 Qdrant 召回出来的一条字段信息。
from app.models.qdrant.column_info_qdrant import ColumnInfoQdrant


async def merge_retrieved_info(
    state: DataAgentState, runtime: Runtime[DataAgentContext]
):
    """
    合并召回信息节点。

    这个节点的核心作用是把前面多个召回节点的结果整合到一起，
    最终形成：
    - table_infos：表信息及其相关字段集合
    - metric_infos：指标信息集合

    前置召回来源包括：
    1. retrieved_columns：字段语义召回结果（通常来自 Qdrant）
    2. retrieved_values：字段值召回结果（通常来自 Elasticsearch）
    3. retrieved_metrics：指标语义召回结果（通常来自 Qdrant）

    为什么要做“合并”？
    因为不同召回源解决的问题不同：
    - 字段召回告诉系统：用户可能提到了哪些字段
    - 值召回告诉系统：用户提到的具体值属于哪些字段
    - 指标召回告诉系统：用户可能提到了哪些业务指标

    最终要把它们统一折叠成：
    - 相关表
    - 相关字段
    - 相关指标
    供后续过滤表信息、过滤指标信息、补充上下文、生成 SQL 使用。
    """

    # 取出流式写入器，用于实时向外汇报当前阶段。
    writer = runtime.stream_writer

    # 输出当前节点阶段信息。
    writer({"stage": "合并召回信息"})

    # 从 state 中取出三个召回节点的结果。
    retrieved_columns = state["retrieved_columns"]
    retrieved_values = state["retrieved_values"]
    retrieved_metrics = state["retrieved_metrics"]

    # 从 runtime.context 中取出 meta MySQL 仓储。
    # 后面会用它补查：
    # - 某个字段的完整元信息
    # - 某张表的元信息
    # - 某张表的关键字段（主键、外键等）
    meta_mysql_repository = runtime.context["meta_mysql_repository"]

    # 最终要返回给后续节点的表信息状态集合
    table_infos: list[TableInfoState] = []

    # 最终要返回给后续节点的指标信息状态集合
    metric_infos: list[MetricInfoState] = []

    # -----------------------------
    # 第 1 步：把字段召回结果整理成一个 column_id -> column_info 的 map
    # -----------------------------
    #
    # retrieved_columns 通常是字段语义召回结果。
    # 这里把它转成字典，便于后续快速按字段 id 去重、补充值、补充字段。
    #
    # 例如：
    # {
    #   "fact_order.order_amount": {...},
    #   "dim_customer.member_level": {...}
    # }
    id_to_column_map: dict[str, ColumnInfoQdrant] = {
        value["id"]: value for value in retrieved_columns
    }

    # -----------------------------
    # 第 2 步：把“值召回结果”合并进字段 map
    # -----------------------------
    #
    # retrieved_values 一般来自 ES。
    # 它表示用户问题中提到的具体值，例如：
    # - 华东
    # - 黄金会员
    # - 苹果
    #
    # 这些值会告诉我们：
    # “这个值属于哪个字段”
    #
    # 所以这里需要把值补充回对应字段的 examples 中。
    for retrieved_value in retrieved_values:
        # 取出该值对应的字段 id
        column_id = retrieved_value["column_id"]

        # 取出实际值本身
        value = retrieved_value["value"]

        # 如果这个字段已经在字段召回结果里了
        if column_id in id_to_column_map:
            # 那就把这个值补充进字段的 examples
            # 这样后续模型在理解字段时，会有更完整的样例值参考。
            if value not in id_to_column_map[column_id]["examples"]:
                id_to_column_map[column_id]["examples"].append(value)

        else:
            # 如果这个字段此前不在字段召回结果中，
            # 说明它是通过“值召回”才被发现的。
            #
            # 这时需要从 meta 数据库中补查该字段的完整元信息。
            column_info: ColumnInfoMySQL = await meta_mysql_repository.get_column_by_id(
                column_id
            )

            # 如果这个值还不在 examples 中，则补进去
            if value not in column_info.examples:
                column_info.examples.append(value)

            # 再把 MySQL 模型转换成 Qdrant 风格的字段对象，
            # 统一放入字段 map 中，便于后续统一处理。
            id_to_column_map[column_id] = _convert_column_info_from_mysql_to_qdrant(
                column_info
            )

    # -----------------------------
    # 第 3 步：把“指标召回结果”涉及到的底层字段也补充进字段 map
    # -----------------------------
    #
    # retrieved_metrics 表示召回到的指标，例如：
    # - GMV
    # - AOV
    #
    # 一个指标会关联到底层字段，例如：
    # GMV -> fact_order.order_amount
    #
    # 所以后面生成 SQL 时，不能只知道“召回了 GMV”，
    # 还要把它依赖的字段补齐。
    for retrieved_metric in retrieved_metrics:
        relevant_columns = retrieved_metric["relevant_columns"]

        # 遍历指标依赖的每一个字段
        for column_id in relevant_columns:
            # 如果这个字段还不在字段 map 中，则从 meta DB 补查
            if column_id not in id_to_column_map:
                column_info: ColumnInfoMySQL = (
                    await meta_mysql_repository.get_column_by_id(column_id)
                )
                id_to_column_map[column_id] = _convert_column_info_from_mysql_to_qdrant(
                    column_info
                )

    # -----------------------------
    # 第 4 步：按 table_id 对字段分组
    # -----------------------------
    #
    # 后续返回给模型的是“表 -> 字段集合”的结构，
    # 所以这里需要先把所有召回到的字段按 table_id 分组。
    #
    # 例如：
    # {
    #   "fact_order": [字段1, 字段2],
    #   "dim_customer": [字段3]
    # }
    table_to_columns_map: dict[str, list[ColumnInfoQdrant]] = {}

    for column in id_to_column_map.values():
        if column["table_id"] not in table_to_columns_map:
            table_to_columns_map[column["table_id"]] = []
        table_to_columns_map[column["table_id"]].append(column)

    # -----------------------------
    # 第 5 步：构造 TableInfoState
    # -----------------------------
    #
    # 对每一张涉及到的表：
    # 1. 查表元信息
    # 2. 把字段信息转换为状态对象
    # 3. 补充关键字段（主键 / 外键等）
    # 4. 封装成 TableInfoState
    for table_id, columns in table_to_columns_map.items():
        # 从 meta DB 取出表元信息
        table_info: TableInfoMySQL = await meta_mysql_repository.get_table_by_id(
            table_id
        )

        # 用于保存这张表最终要暴露给后续节点的字段状态
        column_states: list[ColumnInfoState] = []

        # 记录当前已经加入的字段 id，避免后续补关键字段时重复
        column_state_ids: list[str] = []

        # 先把召回到的字段转成 ColumnInfoState
        for column in columns:
            column_state = _convert_column_info_from_qdrant_to_state(column)
            column_state_ids.append(column["id"])
            column_states.append(column_state)

        # 补充关键字段
        #
        # 为什么要补？
        # 因为用户问题不一定显式提到主键 / 外键，
        # 但后续生成 SQL 时，做表关联通常需要这些字段。
        key_columns = await meta_mysql_repository.get_key_columns_by_table_id(table_id)

        for key_column in key_columns:
            # 只有当前没出现过时才补，避免重复
            if key_column.id not in column_state_ids:
                key_column_state = _convert_column_info_from_mysql_to_state(key_column)
                column_states.append(key_column_state)

        # 构造表级状态对象
        table_info_state = TableInfoState(
            name=table_info.name,
            role=table_info.role,
            description=table_info.description,
            columns=column_states,
        )

        # 加入最终表信息集合
        table_infos.append(table_info_state)

    # -----------------------------
    # 第 6 步：构造 MetricInfoState
    # -----------------------------
    #
    # 把召回到的指标信息转换成后续节点统一使用的状态对象。
    for metric in retrieved_metrics:
        metric_info_state = MetricInfoState(
            name=metric["name"],
            description=metric["description"],
            alias=metric["alias"],
        )
        metric_infos.append(metric_info_state)

    # 记录日志
    logger.info("召回信息合并成功")

    # 返回给后续节点的状态增量
    return {"table_infos": table_infos, "metric_infos": metric_infos}


def _convert_column_info_from_mysql_to_qdrant(
    column_info: ColumnInfoMySQL,
) -> ColumnInfoQdrant:
    """
    把 MySQL 中的字段模型转换成 Qdrant 风格的字段对象。

    使用场景：
    - 某个字段不是从 Qdrant 字段召回中拿到的
    - 而是通过值召回或指标依赖补查出来的
    - 为了统一后续处理，这里把 MySQL 模型转换成与 Qdrant 召回结果同风格的对象
    """
    return ColumnInfoQdrant(
        id=column_info.id,
        name=column_info.name,
        type=column_info.type,
        role=column_info.role,
        examples=column_info.examples,
        description=column_info.description,
        alias=column_info.alias,
        table_id=column_info.table_id,
    )


def _convert_column_info_from_qdrant_to_state(
    column_info: ColumnInfoQdrant,
) -> ColumnInfoState:
    """
    把 Qdrant 召回出来的字段对象，转换成 graph 中流转的 ColumnInfoState。

    注意：
    这里使用的是字典式访问：
    column_info["name"]
    说明当前 Qdrant 返回结果在运行时更像 dict / TypedDict 风格对象。
    """
    return ColumnInfoState(
        name=column_info["name"],
        type=column_info["type"],
        role=column_info["role"],
        description=column_info["description"],
        alias=column_info["alias"],
        examples=column_info["examples"],
    )


def _convert_column_info_from_mysql_to_state(
    column_info: ColumnInfoMySQL,
) -> ColumnInfoState:
    """
    把 MySQL 字段模型转换成 graph 中流转的 ColumnInfoState。

    使用场景：
    - 关键字段（主键 / 外键等）是从 MySQL 中直接补查出来的
    - 需要统一转换成状态对象后再交给后续节点使用
    """
    return ColumnInfoState(
        name=column_info.name,
        type=column_info.type,
        role=column_info.role,
        description=column_info.description,
        alias=column_info.alias,
        examples=column_info.examples,
    )
