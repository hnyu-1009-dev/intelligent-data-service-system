from qdrant_client import AsyncQdrantClient

from app.config.app_config import QdrantConfig, app_config


class QdrantClientManager:
    def __init__(self, config: QdrantConfig):
        # 保存一份 Qdrant 的配置对象。
        # 这个配置通常来自项目统一的配置中心，里面一般会包含：
        # - host：Qdrant 服务地址
        # - port：Qdrant 服务端口
        # 后续如果项目升级，也可能继续扩展：
        # - api_key
        # - https
        # - timeout
        self.config: QdrantConfig = config

        # client 用来保存真正的 Qdrant 异步客户端实例。
        # 一开始先设置为 None，表示“当前还没有完成初始化”。
        #
        # 这里使用类型标注：
        # AsyncQdrantClient | None
        # 含义是：self.client 可能是一个 AsyncQdrantClient，
        # 也可能是 None。
        #
        # 这样写有几个好处：
        # 1. 对阅读代码的人更清晰，知道这个字段不是一开始就可用。
        # 2. IDE 能提供更准确的类型提示。
        # 3. 类型检查工具可以帮助发现未初始化就使用 client 的问题。
        self.client: AsyncQdrantClient | None = None

    def _get_url(self):
        # 组装 Qdrant 服务的访问地址。
        #
        # 比如：
        # self.config.host = "127.0.0.1"
        # self.config.port = 6333
        #
        # 最终返回：
        # "http://127.0.0.1:6333"
        #
        # 虽然当前 init() 中没有直接使用这个方法，
        # 因为 AsyncQdrantClient 支持直接传 host 和 port，
        # 但保留这个方法仍然是一个不错的设计，原因有：
        # 1. 后续如果改成通过 url 初始化，可以直接复用；
        # 2. 调试时可以方便打印连接地址；
        # 3. 如果项目其他地方需要统一获取 Qdrant 地址，可以直接调用这里。
        return f"http://{self.config.host}:{self.config.port}"

    def init(self):
        # 初始化 Qdrant 异步客户端。
        #
        # 这里做的事情不是“立刻发起一次数据库请求”，
        # 而是“创建一个客户端对象”，让后续业务代码可以通过它访问 Qdrant。
        #
        # 为什么不直接在 __init__ 里创建 client，而要单独写一个 init()？
        # 这是后端项目中很常见的“延迟初始化”设计，主要有这些好处：
        #
        # 1. __init__ 只负责构造对象本身，职责更单一；
        # 2. 应用启动时可以统一控制所有外部资源的初始化时机；
        # 3. 测试时更方便，可以只创建 manager 而不连接真实服务；
        # 4. 如果初始化失败，也更容易定位问题发生在 init() 阶段。
        #
        # 这里传入 host 和 port，让客户端知道要连接哪个 Qdrant 服务。
        self.client = AsyncQdrantClient(
            host=self.config.host,
            port=self.config.port
        )

    async def close(self):
        # 关闭 Qdrant 客户端，释放底层资源。
        #
        # 因为这里使用的是异步客户端 AsyncQdrantClient，
        # 所以关闭操作也需要使用 await。
        #
        # 为什么关闭很重要？
        # 1. 释放底层 HTTP 连接或会话资源；
        # 2. 避免资源泄漏；
        # 3. 让服务在关闭时更加优雅和可控。
        #
        # 注意：
        # 如果 self.client 还没有执行过 init()，那么它还是 None，
        # 此时直接调用 self.client.close() 会报错：
        # AttributeError: 'NoneType' object has no attribute 'close'
        #
        # 当前这段代码默认假设：
        # “close() 被调用之前，init() 一定已经成功执行过。”
        #
        # 如果你想写得更健壮一些，可以改成：
        # if self.client is not None:
        #     await self.client.close()
        await self.client.close()


# 创建一个全局的 Qdrant 客户端管理器实例。
#
# 这里把项目配置中的 qdrant 配置传入 manager，
# 这样整个项目都可以通过这个全局对象统一管理 Qdrant 客户端。
#
# 这种写法在后端工程里非常常见，优点包括：
# 1. 统一管理外部依赖资源；
# 2. 避免业务代码里重复创建客户端对象；
# 3. 更适合在应用启动和关闭阶段集中初始化 / 释放资源。
qdrant_client_manager = QdrantClientManager(app_config.qdrant)