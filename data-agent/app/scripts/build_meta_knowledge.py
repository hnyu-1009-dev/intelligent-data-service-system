# argparse：
# Python 标准库中的命令行参数解析模块。
# 它的作用是让我们可以在终端运行脚本时，传入像 --config xxx.yaml 这样的参数，
# 然后在代码里读取这些参数的值。
import argparse

# asyncio：
# Python 标准库中的异步编程库。
# 由于下面的 build() 是一个 async 异步函数，所以需要用 asyncio.run()
# 来启动事件循环并执行这个异步任务。
import asyncio

# Path：
# pathlib 标准库中的路径对象类，用来替代传统的字符串路径拼接。
# 它的优点是：
# 1. 可读性更好
# 2. 跨平台更安全
# 3. 支持很多方便的路径操作
#
# 例如：
# Path("a/b/c.txt")
# 比直接写字符串路径更适合工程代码。
from pathlib import Path

# 导入 embedding_client_manager：
# 这是项目中封装好的“向量/Embedding 服务客户端管理器”。
# 一般用于统一管理 Embedding 模型服务连接，比如：
# - 文本转向量
# - 批量生成向量
#
# manager 的意义在于：
# 1. 统一初始化客户端
# 2. 统一关闭资源
# 3. 避免业务代码里反复创建连接
from app.clients.embedding_client import embedding_client_manager

# 导入 es_client_manager：
# 这是 Elasticsearch 客户端管理器。
# 作用是统一管理 ES 连接，后续可以通过它访问 Elasticsearch，
# 完成索引写入、搜索、删除等操作。
from app.clients.es_client import es_client_manager

# 导入两个 MySQL 客户端管理器：
# - dw_client_manager：通常连接数仓库 / 数据仓库（DW）
# - meta_client_manager：通常连接元数据库（Meta DB）
#
# 这两个 manager 内部一般维护：
# - SQLAlchemy engine
# - session_factory
#
# 后续通过 session_factory() 创建异步数据库会话。
from app.clients.mysql_client import dw_client_manager, meta_client_manager

# 导入 Qdrant 客户端管理器：
# Qdrant 是向量数据库，常用于语义检索、相似度搜索、RAG 等场景。
# 这个 manager 统一管理 Qdrant 客户端实例。
from app.clients.qdrant_client_manager import qdrant_client_manager

# 导入 ValueESRepository：
# Repository（仓储层）负责封装对某种存储介质的具体读写逻辑。
# 这里的 ValueESRepository 表示“面向 Elasticsearch 的值数据访问层”。
# 业务层不直接操作 ES client，而是通过 repository 调用更语义化的方法。
from app.repositories.es.value_es_repository import ValueESRepository

# 导入 DWMySQLRepository：
# 这是面向数据仓库 MySQL 的仓储对象。
# 它会基于传入的 dw_session，封装与 DW 库相关的数据查询逻辑。
from app.repositories.mysql.dw_mysql_repository import DWMySQLRepository

# 导入 MetaMySQLRepository：
# 这是面向元数据库 MySQL 的仓储对象。
# 它会基于传入的 meta_session，封装与元数据相关的数据访问逻辑。
from app.repositories.mysql.meta_mysql_repository import MetaMySQLRepository

# 导入 ColumnQdrantRepository：
# 这是面向 Qdrant 的“字段/列向量仓储”。
# 一般用于把字段信息写入向量库，或者执行字段级别的向量检索。
from app.repositories.qdrant.column_repository_qdrant import ColumnQdrantRepository

# 导入 MetricQdrantRepository：
# 这是面向 Qdrant 的“指标向量仓储”。
# 一般用于把指标信息写入向量库，或者执行指标级别的向量检索。
from app.repositories.qdrant.metric_repository_qdrant import MetricQdrantRepository

# 导入 MetaKnowledgeService：
# Service（服务层）通常负责组织多个 repository / client，
# 完成一个完整业务流程。
#
# 这里的 MetaKnowledgeService 从名字看，应该负责“元知识构建”相关业务，
# 例如：
# - 从数据库中读取元数据
# - 调用 embedding 模型生成向量
# - 把向量写入 Qdrant
# - 把某些值信息写入 ES
from app.service.meta_knowledge_service import MetaKnowledgeService


async def build(meta_config: Path):
    # build() 是整个“构建元知识”的主流程函数。
    #
    # 参数 meta_config 是一个 Path 对象，
    # 表示外部传入的配置文件路径。
    #
    # 之所以定义为 async，是因为这个流程中会涉及：
    # - 异步数据库操作
    # - 异步向量库操作
    # - 异步 ES 操作
    # 所以整个流程需要运行在异步事件循环里。

    # 初始化 DW 数据库客户端管理器。
    # 通常会创建数据库 engine 和 session_factory。
    dw_client_manager.init()

    # 初始化元数据库客户端管理器。
    meta_client_manager.init()

    # 初始化 Embedding 客户端管理器。
    # 后续会用这个客户端调用向量模型服务。
    embedding_client_manager.init()

    # 初始化 Qdrant 客户端管理器。
    # 后续会用这个客户端访问向量数据库。
    qdrant_client_manager.init()

    # 初始化 Elasticsearch 客户端管理器。
    es_client_manager.init()

    # 使用 async with 同时创建两个异步数据库 session：
    # - dw_session：连接数据仓库
    # - meta_session：连接元数据库
    #
    # async with 的好处是：
    # 1. 会话生命周期清晰
    # 2. 用完后自动释放资源
    # 3. 异常时也能更安全地清理上下文
    async with dw_client_manager.session_factory() as dw_session, meta_client_manager.session_factory() as meta_session:
        # 基于 dw_session 创建 DW 仓储对象。
        # 后续针对 DW 数据库的操作，统一通过这个 repository 进行。
        dw_mysql_repository = DWMySQLRepository(dw_session)

        # 基于 meta_session 创建元数据库仓储对象。
        meta_mysql_repository = MetaMySQLRepository(meta_session)

        # 基于 Qdrant 客户端创建“字段向量仓储”。
        # 这里直接把 qdrant_client_manager.client 传进去，
        # 表示 repository 底层通过这个 client 访问 Qdrant。
        column_qdrant_repository = ColumnQdrantRepository(qdrant_client_manager.client)

        # 基于 Qdrant 客户端创建“指标向量仓储”。
        metric_qdrant_repository = MetricQdrantRepository(qdrant_client_manager.client)

        # 取出 embedding 客户端实例。
        # 这里没有再额外包一层 repository，而是直接把 client 传给 service。
        embedding_client = embedding_client_manager.client

        # 基于 ES 客户端创建 Elasticsearch 仓储对象。
        value_es_repository = ValueESRepository(es_client_manager.client)

        # 组装服务层对象 MetaKnowledgeService。
        #
        # 这里体现的是典型的依赖注入思想：
        # Service 不自己创建依赖，而是由外部把 repository / client 传给它。
        #
        # 这样设计的好处：
        # 1. 业务层职责更清晰
        # 2. 测试更方便，可以注入 mock 对象
        # 3. 各层解耦更强
        meta_knowledge_service = MetaKnowledgeService(
            dw_mysql_repository=dw_mysql_repository,
            meta_mysql_repository=meta_mysql_repository,
            embedding_client=embedding_client,
            column_qdrant_repository=column_qdrant_repository,
            metric_qdrant_repository=metric_qdrant_repository,
            value_es_repository=value_es_repository,
        )

        # 调用服务层方法，执行真正的“元知识构建”。
        #
        # 从命名上推断，这一步大概率会做如下事情：
        # 1. 读取配置文件 meta_config
        # 2. 从 DW / Meta DB 中抽取元数据
        # 3. 生成 embedding 向量
        # 4. 写入 Qdrant
        # 5. 把某些值或索引信息写入 Elasticsearch
        await meta_knowledge_service.build_meta_knowledge(meta_config)

    # 当 async with 代码块结束后，数据库 session 会自动退出上下文。
    # 但客户端管理器本身持有的底层资源仍然需要显式关闭。

    # 关闭 DW 数据库客户端资源。
    await dw_client_manager.close()

    # 关闭元数据库客户端资源。
    await meta_client_manager.close()

    # 关闭 Qdrant 客户端资源。
    await qdrant_client_manager.close()

    # 关闭 ES 客户端资源。
    await es_client_manager.close()


if __name__ == "__main__":
    # 只有当这个文件被“直接运行”时，下面的代码才会执行。
    # 如果这个文件只是被其他模块 import，则不会进入这里。
    #
    # 这是 Python 脚本常见的入口写法。

    # 创建命令行参数解析器。
    # ArgumentParser 用于定义程序接受哪些命令行参数。
    parser = argparse.ArgumentParser()

    # 添加一个必填参数：
    # -c 或 --config
    #
    # 运行脚本时可以这样传：
    # python xxx.py -c config.yaml
    # 或
    # python xxx.py --config config.yaml
    #
    # required=True 表示这个参数必须提供，否则程序会报错并提示用法。
    parser.add_argument("-c", "--config", required=True)

    # 解析命令行参数。
    # 解析完成后，可以通过 args.config 读取用户传入的配置文件路径。
    args = parser.parse_args()

    # 使用 asyncio.run() 执行异步主流程。
    #
    # 这里会：
    # 1. 创建事件循环
    # 2. 运行 build(Path(args.config))
    # 3. build 执行结束后关闭事件循环
    #
    # 同时把命令行传入的 config 路径字符串，转换为 Path 对象传给 build()。
    asyncio.run(build(Path(args.config)))
