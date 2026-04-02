from typing import Generic, TypeVar

# AsyncQdrantClient：
# Qdrant 提供的异步客户端。
# 我们通过它来和 Qdrant 向量数据库交互，比如：
# - 检查 collection 是否存在
# - 创建 collection
# - 写入向量点（upsert）
# - 按向量相似度搜索
from qdrant_client import AsyncQdrantClient

# VectorParams：
# 用来定义 collection 中向量的基本配置，比如：
# - 向量维度 size
# - 相似度计算方式 distance
#
# Distance：
# 表示向量距离度量方式。
# 常见有：
# - COSINE：余弦相似度
# - DOT：点积
# - EUCLID：欧式距离
#
# PointStruct：
# 表示 Qdrant 中的一条“点”数据。
# 一条 point 通常包含：
# - id：唯一标识
# - vector：向量
# - payload：附加元数据
from qdrant_client.models import VectorParams, Distance, PointStruct

# app_config：
# 项目全局配置对象。
# 这里主要使用 app_config.qdrant.embedding_size，
# 来指定创建 Qdrant collection 时向量的维度大小。
from app.config.app_config import app_config

# PayloadT：
# 这是一个泛型类型变量。
# 作用是让 BaseQdrantRepository 可以适配不同类型的 payload。
#
# 例如：
# - 字段仓储里，payload 可能是 ColumnInfoQdrant
# - 指标仓储里，payload 可能是 MetricInfoQdrant
#
# 这样 BaseQdrantRepository 就可以作为一个通用基类复用。
PayloadT = TypeVar("PayloadT")


class BaseQdrantRepository(Generic[PayloadT]):
    """
    BaseQdrantRepository 是一个通用的 Qdrant 仓储基类。

    它封装了访问 Qdrant 的几个通用操作：
    1. ensure_collection()：确保 collection 存在
    2. upsert()：批量写入或更新向量点
    3. search()：根据查询向量检索最相似的 payload

    这个类本身不指定具体 collection 名称，
    collection_name 由子类定义。

    例如：
    - ColumnQdrantRepository 可以继承它，并定义字段 collection 名称
    - MetricQdrantRepository 可以继承它，并定义指标 collection 名称
    """

    # collection_name：
    # 这是类属性，表示当前 repository 对应的 Qdrant collection 名称。
    #
    # 这里没有给默认值，说明这个基类本身不直接使用，
    # 而是要求子类去指定具体的 collection_name。
    #
    # 例如子类可能会写：
    # collection_name = "column_info"
    collection_name: str

    def __init__(self, client: AsyncQdrantClient):
        """
        初始化仓储对象。

        参数：
        - client: AsyncQdrantClient
          已经创建好的 Qdrant 异步客户端实例

        说明：
        这个 repository 自己不负责创建 client，
        而是通过依赖注入的方式接收外部传入的 client。
        这样设计的好处是：
        - 解耦
        - 方便测试
        - 便于统一管理 Qdrant 连接
        """
        self.client = client

    async def ensure_collection(self):
        """
        确保当前 repository 对应的 Qdrant collection 存在。

        执行逻辑：
        1. 先检查 collection 是否已经存在
        2. 如果不存在，就创建 collection

        创建时会指定：
        - 向量维度 size
        - 相似度计算方式 distance

        为什么需要这个方法？
        因为向 Qdrant 写入向量前，目标 collection 必须先存在。
        这和 MySQL 里“先建表再插数据”是类似的。
        """
        # 检查 collection 是否已存在
        exist = await self.client.collection_exists(self.collection_name)

        # 如果 collection 不存在，则创建
        if not exist:
            await self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=VectorParams(
                    # 向量维度，来自项目配置
                    # 例如 embedding_size = 1024
                    size=app_config.qdrant.embedding_size,

                    # 相似度度量方式使用余弦相似度
                    # 适合大多数文本 embedding 检索场景
                    distance=Distance.COSINE,
                ),
            )

    async def upsert(
        self,
        ids: list,
        embeddings: list[list[float]],
        payloads: list[PayloadT],
        batch_size: int = 10,
    ):
        """
        批量写入或更新向量点。

        参数：
        - ids:
          每条向量点的唯一标识列表
        - embeddings:
          向量列表，每个元素是一条 embedding 向量
        - payloads:
          附加元数据列表，与 ids / embeddings 一一对应
        - batch_size:
          分批写入大小，默认每批 10 条

        为什么要分批？
        - 避免一次请求太大
        - 降低单次网络压力
        - 减少内存占用
        - 提高稳定性

        注意：
        ids、embeddings、payloads 三个列表必须一一对应，长度相同。
        """
        # 把 id、向量、payload 按位置打包到一起
        # 这样每个元素就是一条完整的 point 数据：
        # (id, embedding, payload)
        zipped = list(zip(ids, embeddings, payloads))

        # 按 batch_size 分批处理
        for i in range(0, len(zipped), batch_size):
            batch = zipped[i:i + batch_size]

            # 构造 Qdrant 需要的 PointStruct 列表
            points = [
                PointStruct(id=id, vector=embedding, payload=payload)
                for id, embedding, payload in batch
            ]

            # 调用 Qdrant 的 upsert 接口
            # upsert 的意思是：
            # - 如果 id 不存在，则插入
            # - 如果 id 已存在，则更新
            await self.client.upsert(
                collection_name=self.collection_name,
                points=points,
            )

    async def search(
        self,
        vector: list[float],
        score_threshold: float = 0.6,
        limit: int = 10,
    ) -> list[PayloadT]:
        """
        根据输入向量进行相似度搜索。

        参数：
        - vector:
          查询向量，通常由 embedding 模型把用户输入文本转换而来
        - score_threshold:
          相似度阈值，低于这个阈值的结果会被过滤掉
        - limit:
          最多返回多少条结果

        返回：
        - payload 列表
          这里只返回匹配到的 payload，不返回向量本身

        使用场景示例：
        - 用户输入“销售额”
        - 先把“销售额”转成向量
        - 再到字段 collection 中搜索最相近的字段
        - 最终返回字段对应的 payload 信息
        """
        # 调用 Qdrant 的查询接口
        # query=vector 表示按这个向量去做相似度检索
        result = await self.client.query_points(
            collection_name=self.collection_name,
            query=vector,
            score_threshold=score_threshold,
            limit=limit,
        )

        # Qdrant 返回的是点对象列表
        # 这里我们只提取每个点里的 payload 返回给上层业务
        return [point.payload for point in result.points]