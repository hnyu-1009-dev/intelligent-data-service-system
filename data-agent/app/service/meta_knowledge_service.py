import uuid

# HuggingFaceEndpointEmbeddings：
# 这是 LangChain 封装的 Embedding 客户端，用来把文本转换成向量。
# 在这个服务里，它主要负责把：
# - 字段名
# - 字段描述
# - 字段别名
# - 指标名
# - 指标描述
# - 指标别名
# 这些文本转成向量，后续写入 Qdrant，供语义检索使用。
from langchain_huggingface import HuggingFaceEndpointEmbeddings

# load_config：
# 项目里的配置加载函数，用于把外部配置文件加载并解析成指定的数据模型对象。
# 这里会把配置文件解析成 MetaConfig。
from app.config.config_loader import load_config

# MetaConfig / TableConfig / MetricConfig：
# 这些是“元数据配置文件”的数据模型。
# 作用是把配置文件中的内容结构化。
#
# 一般理解为：
# - MetaConfig：整个配置文件对象
# - TableConfig：某一张表的配置
# - MetricConfig：某一个指标的配置
from app.config.meta_config import MetaConfig, TableConfig, MetricConfig

# logger：
# 项目统一日志对象，用来记录构建过程中的关键步骤和状态。
from app.core.logging import logger

# ValueInfoES：
# Elasticsearch 中“值信息”的数据模型。
# 它描述某个字段的某个具体值，后续会被写入 ES，做全文检索或精确检索。
from app.models.es.value_info_es import ValueInfoES

# 下面这些是 MySQL 中的元数据模型，对应 meta 数据库中的表结构。
# 它们不是业务数仓里的表，而是“关于表/字段/指标”的元数据对象。
from app.models.mysql.column_info_mysql import ColumnInfoMySQL
from app.models.mysql.column_metric_mysql import ColumnMetricMySQL
from app.models.mysql.metric_info_mysql import MetricInfoMySQL
from app.models.mysql.table_info_mysql import TableInfoMySQL

# 下面这些是 Qdrant 中使用的数据模型。
# 因为 MySQL 和 Qdrant 的存储格式、用途不同，
# 所以这里单独定义了面向 Qdrant 的 payload 结构。
from app.models.qdrant.column_info_qdrant import ColumnInfoQdrant
from app.models.qdrant.metric_info_qdrant import MetricInfoQdrant

# Repository（仓储层）负责封装对底层存储的操作。
#
# ValueESRepository：
# 封装对 Elasticsearch 的索引写入、批量写入、索引存在检查等逻辑。
from app.repositories.es.value_es_repository import ValueESRepository

# DWMySQLRepository：
# 面向 dw 数据库的仓储对象。
# 负责读取真实业务数据，例如字段类型、字段样例值等。
from app.repositories.mysql.dw_mysql_repository import DWMySQLRepository

# MetaMySQLRepository：
# 面向 meta 数据库的仓储对象。
# 负责把表信息、字段信息、指标信息等写入 meta 数据库。
from app.repositories.mysql.meta_mysql_repository import MetaMySQLRepository

# ColumnQdrantRepository / MetricQdrantRepository：
# 面向 Qdrant 的仓储对象。
# 负责创建 collection、写入向量、更新向量等。
from app.repositories.qdrant.column_repository_qdrant import ColumnQdrantRepository
from app.repositories.qdrant.metric_repository_qdrant import MetricQdrantRepository


class MetaKnowledgeService:
    """
    MetaKnowledgeService 是“元数据知识库构建服务”。

    这段代码的核心目标不是处理普通业务请求，而是做一次“知识构建任务”：
    1. 从配置文件里读取要同步的表和指标定义
    2. 到 dw（数据仓库）里获取真实字段类型和示例值
    3. 把表、字段、指标等元数据保存到 meta 数据库
    4. 把字段、指标的文本信息做 embedding 后写入 Qdrant
    5. 把需要同步的字段值写入 Elasticsearch
    6. 最终构建出一套可用于“语义检索 / 智能问数 / NL2SQL”的元知识库

    可以把它理解成：
    - dw：真实业务数据源
    - meta：结构化元数据存储
    - qdrant：向量检索库
    - es：全文检索库
    - embedding_client：文本向量化工具

    而 MetaKnowledgeService 就是把这些组件串起来的“总调度器”。
    """

    def __init__(
        self,
        dw_mysql_repository: DWMySQLRepository,
        meta_mysql_repository: MetaMySQLRepository,
        embedding_client: HuggingFaceEndpointEmbeddings,
        column_qdrant_repository: ColumnQdrantRepository,
        metric_qdrant_repository: MetricQdrantRepository,
        value_es_repository: ValueESRepository,
    ):
        # dw_repository：
        # 负责访问 dw 数据库（真实业务数仓）。
        # 这个仓储通常用来获取：
        # - 表字段类型
        # - 字段样例值
        # - 字段全量或部分值
        self.dw_repository = dw_mysql_repository

        # meta_repository：
        # 负责访问 meta 数据库。
        # 这个仓储用来保存：
        # - 表元数据
        # - 字段元数据
        # - 指标元数据
        # - 字段与指标的关联关系
        self.meta_repository = meta_mysql_repository

        # embedding_client：
        # 负责把文本转换成向量。
        # 后续字段名、字段描述、字段别名、指标名、指标描述、指标别名
        # 都会通过它转成 embedding。
        self.embedding_client = embedding_client

        # column_qdrant_repository：
        # 负责把“字段知识”写入 Qdrant。
        self.column_qdrant_repository = column_qdrant_repository

        # metric_qdrant_repository：
        # 负责把“指标知识”写入 Qdrant。
        self.metric_qdrant_repository = metric_qdrant_repository

        # full_text_repository：
        # 负责把字段值写入 Elasticsearch。
        # 这里命名为 full_text_repository，强调它主要承担全文检索能力。
        self.full_text_repository = value_es_repository

    async def _save_tables_to_meta_db(self, tables: list[TableConfig]):
        """
        把配置文件中的“表定义 + 字段定义”写入 meta 数据库。

        这个方法的主要作用：
        1. 遍历配置文件中的表定义
        2. 为每张表生成 TableInfoMySQL
        3. 查询 dw 数据库，拿到每个字段的真实类型
        4. 查询 dw 数据库，拿到每个字段的样例值
        5. 为每个字段生成 ColumnInfoMySQL
        6. 把表信息和字段信息统一写入 meta 数据库

        为什么要从 dw 里再查一次？
        因为配置文件中通常只描述业务语义，
        而真实字段类型、样例值等更适合直接从真实数据源读取。
        """

        # 用于暂存表级元数据对象
        table_infos: list[TableInfoMySQL] = []

        # 用于暂存字段级元数据对象
        column_infos: list[ColumnInfoMySQL] = []

        # 遍历配置中的每一张表
        for table in tables:
            # 构造表元数据对象
            # id 这里直接使用表名，表示系统内唯一表标识
            table_info = TableInfoMySQL(
                id=table.name,
                name=table.name,
                role=table.role,
                description=table.description,
            )
            table_infos.append(table_info)

            # 从 dw 数据库读取该表所有字段的真实类型
            # 返回值大概率类似：
            # {
            #   "order_id": "VARCHAR(30)",
            #   "order_amount": "FLOAT",
            #   ...
            # }
            column_types = await self.dw_repository.get_column_types(table.name)

            # 遍历这张表在配置中的每个字段定义
            for column in table.columns:
                # 从 dw 数据库中取该字段的一部分样例值（最多 10 个）
                # 用处：
                # 1. 帮助系统理解这个字段里一般存什么
                # 2. 后续用于元数据展示
                # 3. 辅助语义理解
                column_values = await self.dw_repository.get_column_values(
                    table.name, column.name, 10
                )

                # 构造字段元数据对象
                column_info = ColumnInfoMySQL(
                    # 字段 id 采用 “表名.字段名” 形式，保证全局唯一
                    id=f"{table.name}.{column.name}",
                    # 字段名
                    name=column.name,
                    # 字段真实类型，来自 dw 数据库
                    type=column_types[column.name],
                    # 字段语义角色，例如：
                    # - primary_key
                    # - foreign_key
                    # - measure
                    # - dimension
                    role=column.role,
                    # 字段示例值
                    examples=column_values,
                    # 字段业务描述
                    description=column.description,
                    # 字段别名
                    # 例如 order_amount 的别名可能是：
                    # ["销售额", "成交金额", "订单金额"]
                    alias=column.alias,
                    # 所属表 id
                    table_id=table.name,
                )
                column_infos.append(column_info)

        # 把表信息和字段信息保存到 meta 数据库
        #
        # session.begin() 表示开启一个事务，
        # 这样可以保证：
        # - 表信息保存成功
        # - 字段信息也保存成功
        # 要么一起成功，要么一起回滚，避免部分写入。
        async with self.meta_repository.session.begin():
            await self.meta_repository.save_table_infos(table_infos)
            await self.meta_repository.save_column_infos(column_infos)

        # 返回写入过的表信息和字段信息，
        # 方便后续继续同步到 Qdrant / ES
        return table_infos, column_infos

    def _convert_column_info_from_mysql_to_qdrant(
        self, column_info: ColumnInfoMySQL
    ) -> ColumnInfoQdrant:
        """
        把 MySQL 中的字段元数据对象，转换成 Qdrant 使用的 payload 对象。

        为什么要转换？
        因为 MySQL 模型和 Qdrant payload 模型通常是两套不同的数据表示：
        - MySQL 更偏结构化存储
        - Qdrant 更偏向量检索时附带的元信息（payload）
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

    def _convert_metric_info_from_mysql_to_qdrant(
        self, metric_info: MetricInfoMySQL
    ) -> MetricInfoQdrant:
        """
        把 MySQL 中的指标元数据对象，转换成 Qdrant 的 payload 对象。
        """
        return MetricInfoQdrant(
            id=metric_info.id,
            name=metric_info.name,
            description=metric_info.description,
            relevant_columns=metric_info.relevant_columns,
            alias=metric_info.alias,
        )

    async def _sync_columns_to_qdrant(self, columns: list[ColumnInfoMySQL]):
        """
        把字段元数据同步到 Qdrant。

        这个方法的核心思想是：
        同一个字段，不只存一条向量，而是存多条“可检索文本”：
        - 字段名
        - 字段描述
        - 每个字段别名

        这样做的目的是增强召回能力。
        例如用户可能搜索：
        - “销售额”
        - “订单金额”
        - “成交额”
        最终都希望能召回同一个字段。
        """

        # 确保字段对应的 Qdrant collection 已存在。
        # 如果不存在就创建。
        await self.column_qdrant_repository.ensure_collection()

        # records 用来保存待写入的“原始文本记录”
        # 每条记录包含：
        # - id：记录唯一标识（这里先临时生成）
        # - embedding_text：要拿去做向量化的文本
        # - payload：附带的字段元数据
        records: list[dict] = []

        for column_info in columns:
            # 1. 把字段名作为一条向量记录
            records.append(
                {
                    "id": uuid.uuid4(),
                    "embedding_text": column_info.name,
                    "payload": self._convert_column_info_from_mysql_to_qdrant(
                        column_info
                    ),
                }
            )

            # 2. 把字段描述作为一条向量记录
            records.append(
                {
                    "id": uuid.uuid4(),
                    "embedding_text": column_info.description,
                    "payload": self._convert_column_info_from_mysql_to_qdrant(
                        column_info
                    ),
                }
            )

            # 3. 把每个别名也作为一条向量记录
            # 这样自然语言里的业务词更容易召回对应字段
            for alias in column_info.alias:
                records.append(
                    {
                        "id": uuid.uuid4(),
                        "embedding_text": alias,
                        "payload": self._convert_column_info_from_mysql_to_qdrant(
                            column_info
                        ),
                    }
                )

        # 为最终要写入 Qdrant 的点重新生成一组 id
        #
        # 注意：
        # 这里和上面 records 中的 id 是重复设计了。
        # 真正 upsert 用的是这里的 ids，而不是 records 中保存的 id。
        ids = [uuid.uuid4() for _ in records]

        # 存放所有 embedding 向量
        embeddings = []

        # 批量向量化大小
        # 不一次性全部发给 embedding 服务，是为了避免：
        # - 单次请求过大
        # - 服务超时
        # - 内存占用过高
        embedding_batch_size = 20

        # 分批调用 embedding 服务
        for i in range(0, len(records), embedding_batch_size):
            batch_record = records[i : i + embedding_batch_size]

            # 提取这一批的待向量化文本
            batch_embedding_text = [record["embedding_text"] for record in batch_record]

            # 调用异步 embedding 接口，拿到文本向量
            batch_embeddings = await self.embedding_client.aembed_documents(
                batch_embedding_text
            )

            # 把这一批结果累加起来
            embeddings.extend(batch_embeddings)

        # payload 和 records 一一对应
        payloads = [record["payload"] for record in records]

        # 最终把 id、向量、payload 一起写入 Qdrant
        await self.column_qdrant_repository.upsert(ids, embeddings, payloads)

    async def _save_metrics_to_meta_db(
        self, metrics: list[MetricConfig]
    ) -> list[MetricInfoMySQL]:
        """
        把指标信息和字段-指标关联关系保存到 meta 数据库。

        这个方法主要做两件事：
        1. 保存指标本身（metric_info）
        2. 保存指标与相关字段的映射关系（column_metric）

        这里体现出一个重要建模思想：
        - 指标不是单纯字段
        - 指标和字段通常是多对多关系
        """

        # 暂存指标元数据
        metric_infos: list[MetricInfoMySQL] = []

        # 暂存字段-指标关联关系
        column_metrics: list[ColumnMetricMySQL] = []

        for metric in metrics:
            # 构造指标元数据对象
            metric_info = MetricInfoMySQL(
                id=metric.name,
                name=metric.name,
                description=metric.description,
                relevant_columns=metric.relevant_columns,
                alias=metric.alias,
            )
            metric_infos.append(metric_info)

            # 遍历该指标关联的字段
            # 建立字段 <-> 指标 的关联关系
            for column in metric.relevant_columns:
                column_metric = ColumnMetricMySQL(
                    column_id=column, metric_id=metric.name
                )
                column_metrics.append(column_metric)

        # 在一个事务里统一保存
        async with self.meta_repository.session.begin():
            await self.meta_repository.save_metric_infos(metric_infos)
            await self.meta_repository.save_column_metrics(column_metrics)

        return metric_infos

    async def _sync_values_to_es(
        self,
        table_infos: list[TableInfoMySQL],
        column_infos: list[ColumnInfoMySQL],
        meta_config: MetaConfig,
    ):
        """
        把字段值同步到 Elasticsearch。

        这个方法和同步字段/指标到 Qdrant 不同：
        - Qdrant 存的是“字段名/描述/别名”的向量知识
        - ES 存的是“字段的实际值”

        这样设计通常是为了支持：
        - 关键词检索
        - 精确值检索
        - 候选值召回

        例如用户问：
        - “华东地区”
        - “黄金会员”
        - “苹果”
        系统可以先在 ES 中检索值，再映射到字段和表。
        """

        # 确保 ES 索引存在
        await self.full_text_repository.ensure_index()

        # 准备待写入 ES 的值对象
        values: list[ValueInfoES] = []

        # 建立 table_id -> table_name 的映射
        # 方便后面通过字段元数据反推出表名
        table_id2name = {table_info.id: table_info.name for table_info in table_infos}

        # 建立 column_id -> 是否需要同步值 的映射
        #
        # 这里依赖配置文件里每个字段的 sync 属性：
        # - True：该字段的值同步到 ES
        # - False：不同步
        #
        # 这样可以避免把高基数、无意义、过大字段全部同步进去。
        column_id2sync = {}
        for table in meta_config.tables:
            for column in table.columns:
                column_id2sync[f"{table.name}.{column.name}"] = column.sync

        # 遍历所有字段元数据
        for column_info in column_infos:
            table_name = table_id2name[column_info.table_id]
            column_name = column_info.name

            # 看看这个字段是否配置为需要同步值
            sync = column_id2sync[column_info.id]

            if sync:
                # 从 dw 中取这个字段的大量值（上限 100000）
                # 这些值将被写入 ES。
                column_value = await self.dw_repository.get_column_values(
                    table_name, column_name, 100000
                )

                # 把每个实际值包装成一个 ES 文档对象
                values.extend(
                    [
                        ValueInfoES(
                            # 文档 id 由 表名.字段名.值 拼成
                            # 这样便于保证唯一性（前提是值本身适合作为 id 一部分）
                            id=f"{table_name}.{column_name}.{value}",
                            # 实际字段值
                            value=value,
                            # 字段类型
                            type=column_info.type,
                            # 字段唯一 id
                            column_id=column_info.id,
                            # 字段名
                            column_name=column_info.name,
                            # 所属表 id
                            table_id=column_info.table_id,
                            # 所属表名
                            table_name=table_name,
                        )
                        for value in column_value
                    ]
                )

        # 批量写入 ES
        await self.full_text_repository.batch_index(values)

    async def _sync_metrics_to_qdrant(self, metric_infos: list[MetricInfoMySQL]):
        """
        把指标信息同步到 Qdrant。

        和字段同步的思路一致：
        一个指标会拆成多条向量记录：
        - 指标名
        - 指标描述
        - 指标别名

        这样当用户输入业务术语时，更容易通过向量检索召回正确指标。
        """

        # 确保指标 collection 存在
        await self.metric_qdrant_repository.ensure_collection()

        records: list[dict] = []

        for metric_info in metric_infos:
            # 指标名
            records.append(
                {
                    "id": uuid.uuid4(),
                    "embedding_text": metric_info.name,
                    "payload": self._convert_metric_info_from_mysql_to_qdrant(
                        metric_info
                    ),
                }
            )

            # 指标描述
            records.append(
                {
                    "id": uuid.uuid4(),
                    "embedding_text": metric_info.description,
                    "payload": self._convert_metric_info_from_mysql_to_qdrant(
                        metric_info
                    ),
                }
            )

            # 指标别名
            for alias in metric_info.alias:
                records.append(
                    {
                        "id": uuid.uuid4(),
                        "embedding_text": alias,
                        "payload": self._convert_metric_info_from_mysql_to_qdrant(
                            metric_info
                        ),
                    }
                )

        # 生成最终写入 Qdrant 的点 id
        ids = [uuid.uuid4() for _ in records]

        embeddings = []
        embedding_batch_size = 20

        # 分批生成向量
        for i in range(0, len(records), embedding_batch_size):
            batch_record = records[i : i + embedding_batch_size]
            batch_embedding_text = [record["embedding_text"] for record in batch_record]
            batch_embeddings = await self.embedding_client.aembed_documents(
                batch_embedding_text
            )
            embeddings.extend(batch_embeddings)

        payloads = [record["payload"] for record in records]

        # 批量写入 Qdrant
        await self.metric_qdrant_repository.upsert(ids, embeddings, payloads)

    async def build_meta_knowledge(self, config_file):
        """
        构建元数据知识库的总入口。

        这是整个服务最核心的方法，整体流程如下：

        第 1 步：加载配置文件
            - 读取需要处理的表定义和指标定义

        第 2 步：如果配置中有表
            - 保存表元数据到 meta
            - 保存字段元数据到 meta
            - 同步字段信息到 Qdrant
            - 同步字段值到 Elasticsearch

        第 3 步：如果配置中有指标
            - 保存指标信息到 meta
            - 保存指标和字段关系到 meta
            - 同步指标信息到 Qdrant

        第 4 步：输出构建完成日志

        可以把这个方法理解为整个“知识库初始化 / 重建”的 orchestrator（编排器）。
        """

        # 1. 加载配置文件，并解析成 MetaConfig 对象
        meta_config: MetaConfig = load_config(MetaConfig, config_file)
        logger.info("加载元数据配置文件")

        # 如果配置文件里定义了 tables，就开始处理表和字段相关知识
        if meta_config.tables:
            # 2. 保存表信息和字段信息到 meta 数据库
            table_infos, column_infos = await self._save_tables_to_meta_db(
                meta_config.tables
            )
            logger.info("保存表信息和字段信息到meta数据库")

            # 3. 把字段知识同步到 Qdrant
            # 这样后续可以做语义检索，比如通过“销售额”召回对应字段
            await self._sync_columns_to_qdrant(column_infos)
            logger.info("同步字段信息到qdrant")

            # 4. 把字段实际值同步到 ES
            # 这样后续可以通过关键词或具体值进行检索
            await self._sync_values_to_es(table_infos, column_infos, meta_config)
            logger.info("同步字段值到es")

        # 如果配置文件里定义了 metrics，就开始处理指标相关知识
        if meta_config.metrics:
            # 5. 保存指标元数据到 meta 数据库
            metric_infos = await self._save_metrics_to_meta_db(meta_config.metrics)
            logger.info("保存metric信息到meta数据库")

            # 6. 把指标知识同步到 Qdrant
            await self._sync_metrics_to_qdrant(metric_infos)
            logger.info("同步metric信息到qdrant")

        # 7. 全部流程完成
        logger.info("元数据知识库构建完成")
