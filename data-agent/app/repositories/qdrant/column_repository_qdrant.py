# 导入 embedding_client_manager：
# 这是项目中统一管理 Embedding 客户端的对象。
#
# 它的职责通常包括：
# - 根据配置初始化 embedding client
# - 对外暴露 client，供业务代码直接调用
#
# 在这个测试脚本里，会用它把查询文本转成向量。
from app.clients.embedding_client import embedding_client_manager

# 导入 qdrant_client_manager：
# 这是项目中统一管理 Qdrant 客户端的对象。
#
# 它的职责通常包括：
# - 初始化 AsyncQdrantClient
# - 对外暴露 client，供 repository 使用
from app.clients.qdrant_client_manager import qdrant_client_manager

# 导入 ColumnInfoQdrant：
# 这是项目中定义的“字段信息在 Qdrant 中的 payload 模型”。
#
# 一个字段在 Qdrant 中通常会保存：
# - id
# - name
# - type
# - role
# - examples
# - description
# - alias
# - table_id
#
# 这里把它作为泛型参数传给 BaseQdrantRepository，
# 表示这个仓储类主要处理“字段信息”类型的数据。
from app.models.qdrant.column_info_qdrant import ColumnInfoQdrant

# 导入 BaseQdrantRepository：
# 这是项目里封装好的 Qdrant 通用仓储基类。
#
# 它通常已经实现了：
# - ensure_collection()
# - upsert()
# - search()
#
# 子类只需要：
# - 指定 payload 类型
# - 指定 collection_name
#
# 就可以复用父类的通用逻辑。
from app.repositories.qdrant.base_repository_qdrant import BaseQdrantRepository


class ColumnQdrantRepository(BaseQdrantRepository[ColumnInfoQdrant]):
    """
    字段信息的 Qdrant 仓储类。

    这个类继承自 BaseQdrantRepository，
    这里没有重写任何方法，只是指定了：

    1. 当前仓储处理的 payload 类型是 ColumnInfoQdrant
    2. 当前仓储对应的 collection 名称是 data_agent_column

    因此它会直接继承父类的：
    - ensure_collection()
    - upsert()
    - search()

    后续通过这个仓储，就可以操作 Qdrant 中“字段信息”这一类向量数据。
    """
    collection_name = "data_agent_column"


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
        3. data_agent_column 这个 collection 中是否已有字段向量数据
        4. 根据输入 query 是否能召回相关字段信息

        这个测试的整体流程是：
        - 初始化 Embedding 客户端
        - 初始化 Qdrant 客户端
        - 创建字段仓储对象
        - 把用户查询转成向量
        - 到 Qdrant 中搜索相似字段
        - 打印第一条召回结果的字段描述
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

        # 创建字段向量仓储对象。
        #
        # 这里把 qdrant_client_manager.client 注入进去，
        # 使得该仓储可以通过这个 client 操作 Qdrant。
        vector_repository = ColumnQdrantRepository(qdrant_client_manager.client)

        # 模拟一个用户查询。
        #
        # 这个查询更偏向业务自然语言，
        # 并不一定直接等于数据库中的字段名。
        #
        # 例如：
        # - “东北地区” 可能对应地区字段的值
        # - “订单数量” 可能对应 fact_order.order_quantity
        query = "统计一下东北地区的订单数量"

        # 把 query 转换成向量，然后去 Qdrant 中搜索相似字段。
        #
        # 这里调用的是：
        # 1. embedding_client.embed_query(query)
        #    把文本转换成向量
        # 2. vector_repository.search(...)
        #    用这个向量去 Qdrant 的字段 collection 中做相似度检索
        #
        # 返回的 result 通常是一个字段 payload 列表。
        result = await vector_repository.search(embedding_client.embed_query(query))

        # 打印第一条召回结果的 description 字段。
        #
        # 如果检索成功，result[0] 可能是类似这样的结构：
        # {
        #   "id": "fact_order.order_quantity",
        #   "name": "order_quantity",
        #   "description": "订单中商品的购买数量。",
        #   ...
        # }
        #
        # 这里打印 description，可以直观看到召回到的字段业务描述。
        print(result[0]["description"])


    # 启动异步测试函数。
    asyncio.run(test())