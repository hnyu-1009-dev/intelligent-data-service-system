from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


class DWMySQLRepository:
    """
    数据仓库（DW, Data Warehouse）访问仓库类。

    这个类的职责：
    - 封装与数据仓库 MySQL 的交互逻辑
    - 对外提供“按业务语义命名”的方法，而不是让上层直接拼 SQL
    - 让 Service / Agent / 上层业务代码不直接依赖具体 SQL 细节

    为什么要单独抽一个 Repository？
    在后端分层设计中，常见结构是：

    - Controller / API 层：接收请求、返回响应
    - Service 层：组织业务逻辑
    - Repository / DAO 层：专门负责数据库访问

    这样做的好处：
    1. 代码职责更清晰
    2. SQL 集中管理，后续更容易维护
    3. 上层不用关心数据库实现细节
    4. 更方便测试和替换底层存储实现

    这里的 DWMySQLRepository 主要是围绕“数据仓库库表元信息、SQL 校验、SQL 执行”做封装。
    """

    def __init__(self, dw_session: AsyncSession):
        """
        初始化 Repository。

        :param dw_session: SQLAlchemy 异步会话对象 AsyncSession

        这里传入的是一个“已经创建好的数据库会话”。
        Repository 不负责创建连接，而是只负责使用连接。

        这是一种典型的依赖注入（Dependency Injection）思想：
        - Repository 依赖数据库会话
        - 但会话由外部传入，而不是自己内部创建

        好处：
        1. 解耦：Repository 不需要知道连接池、engine、配置文件等细节
        2. 复用：同一个 session 可以在多个 Repository / Service 中共享
        3. 测试方便：测试时可以传入 mock session

        AsyncSession 是 SQLAlchemy 的异步会话对象，适合在 async/await 项目中使用。
        """
        self.dw_session = dw_session

    async def get_column_types(self, table_name: str) -> dict[str, str]:
        """
        获取指定表中每个字段的数据类型。

        :param table_name: 表名，例如 "orders"
        :return: 一个字典，key 是字段名，value 是字段类型
                 例如：
                 {
                     "id": "bigint",
                     "order_no": "varchar(64)",
                     "amount": "decimal(10,2)"
                 }

        实现原理：
        - MySQL 提供 `SHOW COLUMNS FROM 表名` 语法，可以查看表结构
        - 返回结果中通常包含：
            Field   -> 字段名
            Type    -> 字段类型
            Null    -> 是否允许为空
            Key     -> 是否是主键 / 索引字段
            Default -> 默认值
            Extra   -> 额外信息（如 auto_increment）

        例如执行：
            SHOW COLUMNS FROM orders;

        可能得到：
            id         bigint(20)
            order_no   varchar(64)
            amount     decimal(10,2)
            created_at datetime

        然后代码会把它整理成 Python 字典返回。

        注意：
        当前这里使用了 f-string 直接拼接表名：
            text(f"show columns from {table_name}")

        这种写法虽然直观，但如果 table_name 来自外部用户输入，
        会有 SQL 注入风险。

        例如恶意输入：
            table_name = "orders; drop table users;"

        在生产环境中，表名 / 字段名这类“标识符”通常不能直接使用参数绑定，
        所以更安全的做法是：
        1. 对表名做白名单校验
        2. 或者只允许从系统元数据中选取合法表名
        """
        sql = text(f"show columns from {table_name}")
        result = await self.dw_session.execute(sql)

        # result.fetchall() 会把查询结果全部取出，返回一个行对象列表
        # 每一行都包含 SHOW COLUMNS 返回的结构，例如 row.Field, row.Type
        #
        # 最终整理成：
        # {
        #     字段名: 字段类型
        # }
        return {row.Field: row.Type for row in result.fetchall()}

    async def get_column_values(self, table_name: str, column_name: str, limit: int) -> list[Any]:
        """
        获取某张表某个字段的一组去重后的样本值。

        :param table_name: 表名，例如 "orders"
        :param column_name: 字段名，例如 "province"
        :param limit: 最多返回多少个值
        :return: 字段值列表，例如：
                 ["北京", "上海", "广州", "深圳"]

        这个方法通常用于：
        1. 采样字段可能的取值
        2. 给大模型 / Agent 提供字段值示例
        3. 帮助做同义词映射、维度识别、枚举值识别

        SQL 逻辑：
            select
                province as column_value
            from orders
            group by province
            limit 10

        为什么这里用了 group by？
        - 目的是“去重”
        - 按字段分组后，每个不同值只保留一条

        这里也可以写成：
            select distinct province as column_value
            from orders
            limit 10

        `GROUP BY` 和 `DISTINCT` 在这种场景下都能实现“去重取值”的效果。
        一般来说：
        - 想表达“去重字段值”，`DISTINCT` 语义更直接
        - 想表达“按字段聚合”，`GROUP BY` 更通用

        注意事项：
        1. 这里没有排序，所以返回的值不保证固定顺序
        2. 如果字段值很多，只会截取前 limit 个
        3. 表名和字段名仍然存在 SQL 注入风险，需要白名单控制
        """
        sql = text(f"""
            select
                {column_name} as column_value
            from {table_name}
            group by {column_name}
            limit {limit}
        """)

        result = await self.dw_session.execute(sql)

        # 返回的每一行只有一列：column_value
        # 因此最终把所有行中的 column_value 提取为 Python 列表返回
        return [row.column_value for row in result.fetchall()]

    async def get_db_info(self) -> dict[str, str]:
        """
        获取当前数据仓库数据库的基础信息。

        :return: 一个字典，包含：
                 - dialect: 数据库方言名称
                 - version: 数据库版本

                 例如：
                 {
                     "dialect": "mysql",
                     "version": "8.0.36"
                 }

        这里的“方言（dialect）”是 SQLAlchemy 的概念。
        SQLAlchemy 支持多种数据库，每种数据库有自己的 SQL 语法差异，
        因此会用 dialect 来表示底层数据库类型，例如：
        - mysql
        - postgresql
        - sqlite
        - oracle

        代码拆解：
        1. self.dw_session.get_bind()
           获取当前 session 绑定的 engine / connection
        2. .dialect.name
           获取底层数据库类型名称
        3. select version()
           通过数据库函数拿到数据库版本信息

        这个方法常用于：
        - 系统启动自检
        - 诊断数据库环境
        - 动态判断数据库能力
        """
        # 获取当前 session 绑定的数据库方言名称
        dialect = self.dw_session.get_bind().dialect.name

        # MySQL 中 version() 函数可返回数据库版本信息
        sql = text("select version() as version")
        result = await self.dw_session.execute(sql)

        # scalar() 表示取结果集第一行第一列的值
        version = result.scalar()

        return {"dialect": dialect, "version": version}

    async def get_date_info(self) -> dict[str, str]:
        """
        获取数据库当前时间，并整理成日期相关信息。

        :return: 一个字典，包含：
                 - date: 当前日期，格式 YYYY-MM-DD
                 - weekday: 星期几（英文）
                 - quarter: 季度

                 理想示例：
                 {
                     "date": "2026-03-31",
                     "weekday": "Tuesday",
                     "quarter": "1"
                 }

        实现流程：
        1. 执行 `select now() as now`
           从数据库获取当前时间，而不是从 Python 本地时间获取
        2. 将返回的 datetime 格式化成日期、星期、季度等信息

        为什么有时候要从数据库取时间，而不是直接用 Python 的 datetime.now()？
        因为在分布式系统里，应用服务器时间和数据库时间可能存在差异。
        如果你的业务是“基于数据库执行 SQL”，那么使用数据库时间通常更一致。

        重点提醒：
        `strftime("%Q")` 在 Python 标准库中并不是常见、通用的格式符。
        大多数情况下，Python 的 strftime 不支持 `%Q` 表示季度。

        也就是说，这一行：
            now.strftime("%Q")
        在很多环境下是有问题的，可能：
        - 直接报错
        - 或者返回原样字符串 "%Q"

        更稳妥的季度计算方式应该是：
            quarter = (now.month - 1) // 3 + 1

        但这里为了“保留你原始代码逻辑”，我先不改功能，只在注释里指出问题。
        如果你愿意，我下一步可以帮你把这段代码改成“生产可用版”。
        """
        sql = text("select now() as now")
        result = await self.dw_session.execute(sql)

        # 取出数据库返回的当前时间，一般是 datetime 对象
        now = result.scalar()

        return {
            "date": now.strftime("%Y-%m-%d"),   # 例如：2026-03-31
            "weekday": now.strftime("%A"),      # 例如：Tuesday
            "quarter": now.strftime("%Q")       # 注意：这里通常不是 Python 标准支持的季度格式
        }

    async def validate_sql(self, query):
        """
        校验一段 SQL 是否“基本可执行”。

        :param query: 待校验的 SQL 语句
        :return: 无返回值；如果 SQL 有问题，会在 execute 时抛异常

        实现方式：
        - 使用数据库的 `EXPLAIN` 语法对 SQL 做解析
        - 如果 SQL 语法错误、表不存在、字段不存在，通常会报错
        - 如果没有报错，则说明这条 SQL 至少在语法和执行计划层面是可解析的

        例如：
            query = "select * from orders where amount > 100"

        实际执行的是：
            explain select * from orders where amount > 100

        EXPLAIN 是什么？
        - 它不会真正执行查询拿数据
        - 而是让数据库告诉你：这条 SQL 会怎么执行
        - 常用于 SQL 性能分析和语法/结构校验

        这个方法在 AI / NL2SQL 场景里非常常见：
        - 大模型生成 SQL 后，先 validate
        - 如果通过，再真正 execute
        - 这样可以先拦截掉很多明显错误的 SQL

        注意：
        1. 这里同样使用了字符串拼接，query 若来自外部输入要非常谨慎
        2. validate_sql 只能说明“数据库能解析”
           不能保证：
           - 业务逻辑一定正确
           - 查询结果一定符合预期
           - 性能一定好
        """
        await self.dw_session.execute(text(f"explain {query}"))

    async def execute_sql(self, sql):
        """
        执行任意 SQL，并把结果转换成“字典列表”返回。

        :param sql: 要执行的 SQL 字符串
        :return: 查询结果列表，每一行转成一个字典

                 例如 SQL：
                     select id, order_no, amount from orders limit 2

                 返回可能是：
                 [
                     {"id": 1, "order_no": "A001", "amount": 100.50},
                     {"id": 2, "order_no": "A002", "amount": 88.00}
                 ]

        具体过程：
        1. self.dw_session.execute(text(sql))
           执行 SQL
        2. result.mappings()
           把每一行结果包装成“类似字典”的映射对象
        3. fetchall()
           取出全部结果
        4. [dict(row) for row in ...]
           把每一行转换成标准 Python dict

        为什么要返回 list[dict]？
        因为这是一种非常通用、非常适合序列化的结构：
        - 方便转 JSON
        - 方便给前端返回
        - 方便给大模型继续消费
        - 可读性强

        注意：
        1. 这个方法适合执行查询型 SQL（select）
        2. 如果执行 update / insert / delete：
           - 返回结果不一定有行数据
           - 可能需要 commit 才能真正持久化（取决于 session 配置和事务管理）
        3. 当前方法没有显式 commit
           说明它更偏向“查询执行器”
        4. 任意 SQL 执行风险很高，生产环境一般需要：
           - 限制只能执行 select
           - 或做 SQL AST 解析 / 白名单校验
           - 防止危险语句（drop, delete, truncate, update 等）
        """
        result = await self.dw_session.execute(text(sql))

        # result.mappings().fetchall() 的效果：
        # 把查询结果的每一行转成“列名 -> 值”的映射结构
        #
        # 例如一行原始结果可能是：
        # (1, 'A001', 100.5)
        #
        # mappings() 后更像：
        # {'id': 1, 'order_no': 'A001', 'amount': 100.5}
        #
        # 最后再显式 dict(row)，确保得到标准 Python 字典
        return [dict(row) for row in result.mappings().fetchall()]