import asyncio

# AsyncElasticsearch 是 Elasticsearch 官方 Python 客户端提供的异步版本客户端。
# 使用它可以在 asyncio 异步框架下执行 ES 请求，避免阻塞事件循环。
from elasticsearch import AsyncElasticsearch

# 这里引入的是你项目中封装好的 ES 客户端管理器。
# 一般这类 manager 的职责是：
# 1. 初始化连接
# 2. 持有全局 client
# 3. 在应用关闭时统一释放资源
from app.clients.es_client import es_client_manager

# ValueInfoES 是你项目中定义的 ES 文档模型/类型标注。
# 从当前代码看，它大概率是 TypedDict / dict 类型别名 / Pydantic 模型中的一种。
# 这里主要用于：
# 1. 约束索引文档的数据结构
# 2. 提高代码可读性
# 3. 为 IDE 提供类型提示
from app.models.es.value_info_es import ValueInfoES


class ValueESRepository:
    """
    ValueESRepository
    =================
    这是一个“仓储层（Repository）”类，职责是封装与 Elasticsearch 中
    value 数据相关的所有读写操作。

    在后端分层设计里，Repository 通常负责：
    - 面向数据存储进行操作
    - 隔离业务层与底层存储实现
    - 统一管理索引、查询、写入等逻辑

    这样做的好处：
    1. 业务层不需要关心 ES 的 API 细节
    2. 如果以后存储实现变化，改动集中在 Repository 层
    3. 便于测试、维护和扩展
    """

    # Elasticsearch 中的索引名。
    # 你可以把索引理解成关系型数据库里的“表”。
    # 虽然 ES 和 MySQL 的底层结构完全不同，但从使用角度上可以这样类比理解。
    es_index_name = "data_agent"

    # 索引的字段映射（mappings）。
    # 它类似于数据库表结构定义，用于告诉 ES：
    # 每个字段叫什么、是什么类型、如何建立索引、如何分词等。
    es_index_mappings = {
        "properties": {
            # keyword 类型：
            # 不分词，适合精确匹配、过滤、聚合，例如 id、类型编号等字段。
            "id": {"type": "keyword"},

            # text 类型：
            # 会参与全文检索，并配合 analyzer 进行分词。
            # 这里配置了 ik_max_word，说明你部署的 ES 中安装了 IK 中文分词插件。
            #
            # analyzer: 建立索引时的分词器
            # search_analyzer: 查询时的分词器
            #
            # 这里两者都设为 ik_max_word，表示索引和搜索都采用较细粒度的中文分词策略。
            "value": {"type": "text", "analyzer": "ik_max_word", "search_analyzer": "ik_max_word"},

            # 以下这些字段都使用 keyword，说明它们主要用于精确存储和过滤，
            # 而不是用于分词检索。
            "type": {"type": "keyword"},
            "column_id": {"type": "keyword"},
            "column_name": {"type": "keyword"},
            "table_id": {"type": "keyword"},
            "table_name": {"type": "keyword"},
        }
    }

    def __init__(self, es_client: AsyncElasticsearch):
        """
        构造函数：注入一个 AsyncElasticsearch 客户端实例。

        这里采用的是“依赖注入”的设计思想：
        - Repository 不自己创建 ES 连接
        - 而是由外部把已经初始化好的 client 传进来

        好处：
        1. 降低耦合
        2. 方便测试（测试时可以传入 mock client）
        3. 便于统一管理连接生命周期
        """
        self.es_client = es_client

    async def ensure_index(self):
        """
        确保索引存在；如果不存在，则创建索引。

        这一步通常会在应用启动阶段、首次写入前执行。
        作用类似于：
        - MySQL 中的“如果表不存在则建表”
        - 系统初始化时自动准备基础存储结构

        为什么要先判断 exists 再 create？
        - 避免重复创建导致报错
        - 保证幂等性（多次执行结果一致）
        """
        if not await self.es_client.indices.exists(index=self.es_index_name):
            await self.es_client.indices.create(
                index=self.es_index_name,
                mappings=self.es_index_mappings
            )

    async def batch_index(self, docs: list[ValueInfoES], batch_size: int = 10):
        """
        批量写入文档到 Elasticsearch。

        参数：
        - docs: 待写入的文档列表
        - batch_size: 每次 bulk 写入的批次大小，默认 10

        为什么要做批量写入？
        - 如果一条一条调用 index 接口，网络开销大、性能差
        - bulk API 可以显著提升写入效率
        - 这在数据同步、初始化构建索引时很常见

        这里的实现思路：
        1. 按 batch_size 对 docs 分批
        2. 每批构造 bulk 所需的 operations
        3. 调用 ES bulk 接口提交
        """

        # range(0, len(docs), batch_size) 的含义：
        # 从 0 开始，每次跳 batch_size 个位置，用于实现分批切片。
        for i in range(0, len(docs), batch_size):
            # 取出当前批次的数据
            batch = docs[i:i + batch_size]

            # operations 是 bulk API 所需的操作数组。
            # ES bulk 的格式通常是“操作描述 + 文档内容”交替出现。
            operations = []

            for doc in batch:
                # 第一条是操作元信息，告诉 ES：
                # - 当前要执行 index 操作
                # - 写入哪个索引
                # - 文档的 _id 是什么
                operations.append({
                    "index": {
                        "_index": self.es_index_name,
                        "_id": doc["id"]
                    }
                })

                # 第二条是真正的文档内容。
                # 这份 doc 会被写入到指定索引中。
                operations.append(doc)

            # 调用 bulk 接口执行批量写入。
            #
            # 注意：
            # 这里代码默认认为写入一定成功，但在真实生产环境中，
            # 通常建议进一步检查返回结果中的 errors 字段，
            # 因为 bulk 即使 HTTP 请求成功，也不代表每条文档都写入成功。
            await self.es_client.bulk(operations=operations)

    async def query(
        self,
        query: str,
        score_threshold: float = 0.6,
        limit: int = 10
    ) -> list[ValueInfoES]:
        """
        基于 value 字段执行全文检索。

        参数：
        - query: 用户输入的查询文本
        - score_threshold: 最低相关性分数，小于该分数的结果会被过滤
        - limit: 最多返回多少条结果

        返回值：
        - 返回命中的文档列表，每条文档都符合 ValueInfoES 结构

        这里使用的是 match 查询：
        - match 会对查询文本进行分词
        - 然后与 text 类型字段（这里是 value）进行相关性匹配
        - 非常适合自然语言检索、关键词召回等场景

        例如：
        用户输入：“统计一下手机产品的销量”
        ES 会对这句话做中文分词，然后去 value 字段里找相关内容。
        """

        # 构造 ES 查询 DSL。
        # 这里是最基础的全文匹配查询：
        # 在 value 字段上执行 match 检索。
        es_query = {
            "match": {
                "value": query
            }
        }

        # 调用 search 接口执行查询。
        #
        # 参数说明：
        # - index: 指定查询哪个索引
        # - query: 查询条件
        # - min_score: 最低相关性分数阈值
        # - size: 返回结果条数限制
        resp = await self.es_client.search(
            index=self.es_index_name,
            query=es_query,
            min_score=score_threshold,
            size=limit
        )

        # ES 返回结果里，真正的命中文档在：
        # resp["hits"]["hits"]
        #
        # 这里使用 get 的好处是更稳健：
        # 即使 hits 字段不存在，也不会直接抛 KeyError，而是返回默认值 []。
        hits = resp.get("hits", {}).get("hits", [])

        # 用于收集最终结果
        results: list[ValueInfoES] = []

        for hit in hits:
            # 每条命中结果一般包含：
            # - _index
            # - _id
            # - _score
            # - _source
            #
            # _source 才是你原始写进去的文档内容。
            source = hit["_source"]
            results.append(source)

        return results


# 这个判断表示：
# 只有当当前文件被“直接运行”时，下面的测试代码才会执行；
# 如果它是被别的模块 import 进来的，则不会执行。
#
# 这是 Python 中常见的调试/测试入口写法。
if __name__ == '__main__':
    async def test():
        """
        一个简单的本地异步测试函数，用来验证 Repository 是否能正常查询 ES。

        流程：
        1. 初始化 ES 客户端
        2. 创建 Repository 实例
        3. 执行查询
        4. 打印结果
        5. 关闭 ES 客户端连接
        """

        # 初始化 ES 客户端管理器。
        # 一般这里会完成：
        # - 创建 AsyncElasticsearch 实例
        # - 建立连接参数配置
        # - 挂载到 manager.client
        es_client_manager.init()

        # 取出底层 ES client
        es_client = es_client_manager.client

        # 创建仓储对象，供后续查询使用
        full_text_repository = ValueESRepository(es_client)

        # 模拟一个自然语言查询
        query = "统计一下手机产品的销量"

        # 执行查询并打印结果
        print(await full_text_repository.query(query=query))

        # 关闭 ES 客户端，释放底层连接资源
        await es_client_manager.close()


    # asyncio.run() 用来启动一个异步程序入口。
    # 它会创建事件循环并执行 test() 这个协程函数。
    asyncio.run(test())