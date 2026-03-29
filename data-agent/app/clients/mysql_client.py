import asyncio

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

from app.config.app_config import DBConfig, app_config


class MySQLClientManager:
    def __init__(self, db_config: DBConfig):
        # engine 是 SQLAlchemy 的异步数据库引擎，负责维护连接池并与 MySQL 通信。
        self.engine = None
        # session_factory 是 AsyncSession 的工厂，业务侧通常从这里创建 session。
        self.session_factory = None
        # 保留整份数据库配置，后续构造连接串时统一从这里取值。
        self.db_config = db_config

    def _get_url(self):
        # SQLAlchemy 异步 MySQL 连接串格式：
        # mysql+asyncmy://user:password@host:port/database?charset=utf8mb4
        # 这里使用 asyncmy 作为异步驱动，并显式指定 utf8mb4 以支持完整 UTF-8 字符集。
        return (
            f"mysql+asyncmy://{self.db_config.user}:{self.db_config.password}"
            f"@{self.db_config.host}:{self.db_config.port}/{self.db_config.database}"
            f"?charset=utf8mb4"
        )

    def init(self):
        # 初始化异步引擎。通常在应用启动阶段执行一次，后续 session 都复用这个连接池。
        self.engine = create_async_engine(
            self._get_url(),
            # 常驻连接池大小。
            pool_size=10,
            # 连接池不够时允许额外创建的临时连接数。
            max_overflow=20,
            # 每次取连接前先做一次探测，避免拿到失效连接。
            pool_pre_ping=True,
        )
        # async_sessionmaker 本身不是 session，而是一个可重复创建 AsyncSession 的工厂。
        self.session_factory = async_sessionmaker(
            bind=self.engine,
            # 关闭自动 flush，避免查询前隐式把内存改动写入数据库。
            autoflush=False,
            # commit 后对象属性不失效，减少提交后再次访问字段时的额外查询。
            expire_on_commit=False,
        )

    async def close(self):
        # 释放引擎内部维护的连接池，通常在应用停止时调用。
        await self.engine.dispose()


# 预先创建两套数据库管理器：
# dw_client_manager 面向数仓/业务库，meta_client_manager 面向元数据库。
# 注意这里只是创建管理器对象，真正建立连接要等调用 init() 之后才会发生。
dw_client_manager = MySQLClientManager(app_config.db_dw)
meta_client_manager = MySQLClientManager(app_config.db_meta)


if __name__ == '__main__':
    async def test():
        # 本地调试入口：初始化连接后，手动执行一条简单 SQL 验证数据库是否可用。
        dw_client_manager.init()
        async with AsyncSession(dw_client_manager.engine) as session:
            # 这里直接执行原生 SQL，show tables 常用于快速验证连通性和访问权限。
            result = await session.execute(text("show tables"))
            # mappings() 会把结果行转成类似字典的结构，fetchall() 一次性取出所有结果。
            print(result.mappings().fetchall())


    asyncio.run(test())
