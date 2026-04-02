# 导入 embedding_client_manager：
# 这是项目中统一管理 Embedding 客户端的对象。
#
# 它通常负责：
# - 根据配置初始化 embedding client
# - 对外暴露 client，供业务代码直接调用
#
# 在这个测试脚本里，会用它把用户查询文本转换成向量。
from app.clients.embedding_client import embedding_client_manager

# 导入 qdrant_client_manager：
# 这是项目中统一管理 Qdrant 客户端的对象。
#
# 它通常负责：
# - 初始化 AsyncQdrantClient
# - 对外暴露 client，供 repository 使用
from app.clients.qdrant_client_manager import qdrant_client_manager

# 导入 MetricInfoQdrant：
# 这是项目中定义的“指标信息在 Qdrant 中的 payload 模型”。
#
# 一个指标在 Qdrant 中通常会保存：
# - id
# - name
# - description
# - relevant_columns
# - alias
#
# 这里把它作为泛型参数传给 BaseQdrantRepository，
# 表示这个仓储类主要处理“指标信息”类型的数据。
from app.models.qdrant.metric_info_qdrant import MetricInfoQdrant

# 导入 BaseQdrantRepository：
# 这是项目中封装好的 Qdrant 通用仓储基类。
#
# 它通常已经实现了：
# - ensure_collection()：确保 collection 存在
# - upsert()：批量写入或更新向量点
# - search()：根据查询向量检索相似数据
#
# 子类只需要：
# - 指定 payload 类型
# - 指定 collection_name
#
# 就可以复用父类的通用逻辑。
from app.repositories.qdrant.base_repository_qdrant import BaseQdrantRepository


class MetricQdrantRepository(BaseQdrantRepository[MetricInfoQdrant]):
    """
    指标信息的 Qdrant 仓储类。

    这个类继承自 BaseQdrantRepository，
    这里没有重写任何方法，只是指定了：

    1. 当前仓储处理的 payload 类型是 MetricInfoQdrant
    2. 当前仓储对应的 collection 名称是 data_agent_metric

    因此它会直接继承父类的：
    - ensure_collection()
    - upsert()
    - search()

    后续通过这个仓储，就可以操作 Qdrant 中“指标信息”这一类向量数据。
    """
    collection_name = "data_agent_metric"


if __name__ == "__main__":
    # 导入 asyncio：
    # 因为下面要运行异步函数 test()，
    # 所以需要通过 asyncio.run() 启动事件循环。
    import asyncio


    async def test():
        """
        一个简单的本地测试函数，用来验证：

        1. Embedding 服务是否可用
        2. Qdrant 客户端是否可用
        3. data_agent_metric 这个 collection 中是否已有指标向量数据
        4. 根据输入 query 是否能召回相关指标信息

        这个测试的整体流程是：
        - 初始化 Embedding 客户端
        - 初始化 Qdrant 客户端
        - 创建指标仓储对象
        - 把用户查询转成向量
        - 到 Qdrant 中搜索相似指标
        - 打印第一条召回结果的指标名称
        """

        # 初始化 Embedding 客户端管理器。
        # 调用 init() 后，embedding_client_manager.client 才可用。
        embedding_client_manager.init()

        # 取出真正的 embedding client。
        # 后续会用它把 query 文本转换成向量。
        embedding_client = embedding_client_manager.client

        # 初始化 Qdrant 客户端管理器。
        # 调用 init() 后，qdrant_client_manager.client 才可用。
        qdrant_client_manager.init()

        # 创建指标向量仓储对象。
        #
        # 这里把 qdrant_client_manager.client 注入进去，
        # 使得该仓储可以通过这个 client 操作 Qdrant。
        metric_qdrant_repository = MetricQdrantRepository(qdrant_client_manager.client)

        # 模拟一个用户查询。
        #
        # “销售总额”在数据库里通常不是一个原始字段名，
        # 它更像一个业务指标语义表达。
        #
        # 在你的项目里，这类表达很可能会召回到：
        # - GMV
        # 或其他配置过的指标别名
        query = "统计一下销售总额"

        # 把 query 转换成向量，然后去 Qdrant 中搜索相似指标。
        #
        # 这里调用的是：
        # 1. embedding_client.embed_query(query)
        #    把文本转换成向量
        # 2. metric_qdrant_repository.search(...)
        #    用这个向量去 Qdrant 的指标 collection 中做相似度检索
        #
        # 返回的 result 通常是一个指标 payload 列表。
        result = await metric_qdrant_repository.search(
            embedding_client.embed_query(query)
        )

        # 打印第一条召回结果的指标名称。
        #
        # 如果检索成功，result[0] 可能是类似这样的结构：
        # {
        #   "id": "GMV",
        #   "name": "GMV",
        #   "description": "全称Gross Merchandise Value，表示所有订单的成交金额总和。",
        #   "relevant_columns": ["fact_order.order_amount"],
        #   "alias": ["成交总额", "订单总额"]
        # }
        #
        # 这里打印 name，可以直观看到当前 query 最终召回到了哪个指标。
        print(result[0]["name"])


    # 启动异步测试函数。
    asyncio.run(test())