from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.mysql.column_info_mysql import ColumnInfoMySQL
from app.models.mysql.column_metric_mysql import ColumnMetricMySQL
from app.models.mysql.metric_info_mysql import MetricInfoMySQL
from app.models.mysql.table_info_mysql import TableInfoMySQL


class MetaMySQLRepository:
    """
    元数据库（Meta DB）访问仓库类。

    这个 Repository 主要负责操作“元数据表”，而不是业务事实数据表。

    你可以这样区分两个库：

    1. DW（Data Warehouse，数据仓库）
       - 里面存放真实业务数据
       - 例如订单表、用户表、销售表
       - 查询的是“业务数据”

    2. Meta（Metadata，元数据库）
       - 里面存放“描述业务数据的数据”
       - 例如：
         - 有哪些表
         - 每张表有哪些字段
         - 字段的中文含义是什么
         - 指标和字段之间是什么关系
       - 查询的是“元信息”

    在你的项目里，MetaMySQLRepository 的职责就是：
    - 向元数据库保存表信息、字段信息、指标信息、字段-指标关系
    - 按 id 查询某个表、某个字段
    - 查询某张表中的关键字段（主键、外键）

    这类 Repository 在智能数仓 / 智能问数 / NL2SQL 项目里非常重要，
    因为模型要先理解“数据库结构”，才能进一步生成 SQL。
    """

    def __init__(self, meta_session: AsyncSession):
        """
        初始化元数据库 Repository。

        :param meta_session: 元数据库的异步会话对象 AsyncSession

        这里的 session 是通过外部注入进来的，而不是 Repository 自己创建。

        为什么这么设计？
        这是一种典型的依赖注入（Dependency Injection）方式。

        好处：
        1. Repository 只关心“怎么操作数据库”
        2. 不关心数据库连接怎么创建
        3. 上层可以统一管理事务、连接池、commit / rollback
        4. 更容易测试和替换实现

        注意：
        Repository 通常不负责 commit。
        它只负责 add / query。
        真正的提交事务一般在 service 层或者 session 上下文管理层完成。
        """
        self.session = meta_session

    async def save_table_infos(self, table_infos: list[TableInfoMySQL]):
        """
        批量保存表信息对象。

        :param table_infos: TableInfoMySQL 对象列表

        这里的 TableInfoMySQL 一般表示“表元信息”模型，
        例如一张表可能包含：
        - 表 id
        - 表名
        - 表角色（事实表 / 维表）
        - 表描述
        - 其他补充元信息

        session.add_all(...) 的作用：
        - 将这一批 ORM 对象加入当前 SQLAlchemy Session
        - 这一步只是“放入待提交队列”
        - 还没有真正写入数据库
        - 只有后续执行 commit() 才会真正持久化到 MySQL

        也就是说，你要区分两个阶段：

        第一阶段：add / add_all
            把对象放进 session 的工作单元（Unit of Work）

        第二阶段：commit
            真正提交事务，把数据写入数据库

        例如：
            repo.save_table_infos([...])
            await session.commit()

        这样数据才真正落库。
        """
        self.session.add_all(table_infos)

    async def save_column_infos(self, column_infos: list[ColumnInfoMySQL]):
        """
        批量保存字段信息对象。

        :param column_infos: ColumnInfoMySQL 对象列表

        这些对象通常对应“字段元信息”，例如：
        - 字段 id
        - 所属表 id
        - 字段名
        - 字段角色（维度 / 度量 / 主键 / 外键）
        - 字段描述
        - 字段别名

        为什么要单独存字段元数据？
        因为在智能问数系统里，字段是最核心的语义单元之一。

        用户问：
            “统计每个省份的销售额”

        系统必须知道：
        - “省份”可能对应哪个字段
        - “销售额”可能对应哪个指标/字段
        - 这些字段来自哪张表

        所以字段元信息是 SQL 生成的重要基础。
        """
        self.session.add_all(column_infos)

    async def save_metric_infos(self, metric_infos: list[MetricInfoMySQL]):
        """
        批量保存指标信息对象。

        :param metric_infos: MetricInfoMySQL 对象列表

        指标（Metric）通常表示“可统计、可聚合的业务含义”，例如：
        - 销售额
        - 订单量
        - 用户数
        - 客单价

        指标和字段不完全一样：

        字段（Column）是物理存储层概念：
        - amount
        - user_id
        - province

        指标（Metric）是业务语义层概念：
        - 销售总额 = sum(amount)
        - 订单数 = count(order_id)
        - 用户数 = count(distinct user_id)

        在智能分析系统中，指标元信息通常非常重要，
        因为用户问的是“业务概念”，系统最后要把业务概念翻译成 SQL。
        """
        self.session.add_all(metric_infos)

    async def save_column_metrics(self, column_metrics: list[ColumnMetricMySQL]):
        """
        批量保存“字段-指标关系”对象。

        :param column_metrics: ColumnMetricMySQL 对象列表

        这个表一般用于描述：
        - 哪些字段和哪些指标存在关联
        - 某个指标依赖哪些字段
        - 某些字段可以参与哪些指标计算

        例如：
        - 指标“销售额”可能关联字段 amount
        - 指标“订单数”可能关联字段 order_id
        - 指标“地区销售额”可能关联 amount + province

        为什么需要这种关系表？
        因为很多系统里，表与字段、字段与指标之间不是简单的一对一关系，
        而是多对多关系。

        所以通常会用一张中间关系表来描述映射关系。
        """
        self.session.add_all(column_metrics)

    async def get_column_by_id(self, column_id: str) -> ColumnInfoMySQL | None:
        """
        根据字段主键 id 查询单个字段对象。

        :param column_id: 字段 id
        :return:
            - 如果找到，返回 ColumnInfoMySQL 对象
            - 如果找不到，返回 None

        这里使用的是：
            await self.session.get(ModelClass, primary_key)

        这是 SQLAlchemy ORM 里按主键查询单条记录最直接的方式。

        它的语义很像：
            select * from column_info where id = xxx

        但它是 ORM 风格的，不需要自己写 SQL。

        返回值为什么是 `ColumnInfoMySQL | None`？
        因为数据库中未必一定存在这个 id 对应的记录。

        所以调用方要有这种意识：
            column = await repo.get_column_by_id("xxx")
            if column is None:
                # 没查到
        """
        return await self.session.get(ColumnInfoMySQL, column_id)

    async def get_table_by_id(self, table_id) -> TableInfoMySQL | None:
        """
        根据表主键 id 查询单个表对象。

        :param table_id: 表 id
        :return:
            - 如果找到，返回 TableInfoMySQL 对象
            - 如果找不到，返回 None

        这个方法和 get_column_by_id 是一样的思路，
        只是查询的模型换成了 TableInfoMySQL。

        一般用于：
        - 已知 table_id，回查表的详细元信息
        - 从字段表中拿到 table_id 后，再查所属表
        """
        return await self.session.get(TableInfoMySQL, table_id)

    async def get_key_columns_by_table_id(self, table_id) -> list[ColumnInfoMySQL]:
        """
        查询指定表中所有“关键字段”。

        :param table_id: 表 id
        :return: ColumnInfoMySQL 对象列表

        这里定义的“关键字段”是：
        - primary_key：主键
        - foreign_key：外键

        为什么主键、外键很重要？
        因为它们在自动生成 SQL、自动推断表关联关系时非常关键。

        例如：
        - 主键可以标识表中的唯一记录
        - 外键可以连接其他表
        - 系统可以利用主外键关系自动推断 JOIN 条件

        例如：
            orders.customer_id  ->  customer.id

        那么系统在做多表查询时，就可能自动生成：
            join customer on orders.customer_id = customer.id

        --------------------------------------------------
        这段实现用了两种 SQLAlchemy 能力的结合：
        1. text(...)                  -> 原生 SQL
        2. select(Model).from_statement(sql)
                                   -> 告诉 ORM：把原生 SQL 的结果映射成模型对象
        --------------------------------------------------

        先看原生 SQL：

            select *
            from column_info
            where table_id = :table_id
              and role in ('primary_key', 'foreign_key')

        它的意思是：
        - 从字段元信息表 column_info 中查询
        - 找到属于指定 table_id 的字段
        - 并且这些字段的 role 必须是 primary_key 或 foreign_key

        这里的 `:table_id` 是命名参数占位符，
        它不是直接字符串拼接，而是安全参数绑定方式。

        相比这种危险写法：
            f"where table_id = '{table_id}'"

        当前写法更安全：
            where table_id = :table_id

        然后执行时再传：
            {"table_id": table_id}

        这样数据库驱动会负责参数绑定，降低 SQL 注入风险。

        --------------------------------------------------
        为什么还要写：
            select(ColumnInfoMySQL).from_statement(sql)
        --------------------------------------------------

        因为单纯 text(sql) 执行后，默认拿到的是普通结果行，
        不是 ORM 模型对象。

        这里作者希望最终返回的是：
            list[ColumnInfoMySQL]

        所以用了：
            select(ColumnInfoMySQL).from_statement(sql)

        它的意思是：
        “把这段原生 SQL 的查询结果，当成 ColumnInfoMySQL 模型来映射。”

        于是最后：
            result.scalars().all()

        就能得到一个 ORM 对象列表，而不是普通元组列表。

        --------------------------------------------------
        scalars().all() 是什么意思？
        --------------------------------------------------

        假设查询结果本质上是一堆 ColumnInfoMySQL 对象，
        那么：

        - result.scalars()
          表示取结果中的“标量值”
          在这里其实就是每一行对应的 ORM 对象

        - .all()
          表示全部取出，变成 Python list

        所以最终返回值类似：
            [
                ColumnInfoMySQL(...),
                ColumnInfoMySQL(...),
                ColumnInfoMySQL(...)
            ]
        """
        sql = text("""
                   select *
                   from column_info
                   where table_id = :table_id
                     and role in ('primary_key', 'foreign_key')
                   """)

        # 这里不是直接执行 text(sql) 后取普通行结果，
        # 而是通过 from_statement 告诉 SQLAlchemy：
        # “请把这段原生 SQL 的结果映射成 ColumnInfoMySQL 模型对象”
        query = select(ColumnInfoMySQL).from_statement(sql)

        # 执行查询时传入绑定参数 {"table_id": table_id}
        # 这样数据库会安全地把 table_id 代入 SQL，而不是直接字符串拼接
        result = await self.session.execute(query, {"table_id": table_id})

        # scalars()：取出结果中的 ORM 对象本身
        # all()：全部转成列表返回
        return result.scalars().all()