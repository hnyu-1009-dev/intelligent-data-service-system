# dataclass：
# Python 标准库 dataclasses 中提供的装饰器。
# 它的作用是把一个普通类快速变成“数据类”。
#
# 使用 @dataclass 后，Python 会自动帮你生成：
# - __init__()：构造函数
# - __repr__()：打印对象时更友好的显示
# - __eq__()：对象比较方法
#
# 这种写法非常适合拿来定义“配置模型”“数据载体对象”。
from dataclasses import dataclass

# Path：
# pathlib 标准库中的路径类，用来表示文件路径。
# 相比直接拼接字符串路径，Path 更清晰，也更适合跨平台开发。
#
# 例如：
# Path("conf/meta_config.yaml")
from pathlib import Path

# OmegaConf：
# Python 中常见的配置管理库，适合处理 YAML 配置文件。
# 它支持：
# - 加载 YAML
# - 结构化配置
# - 配置合并
# - 配置校验
from omegaconf import OmegaConf


@dataclass
class ColumnConfig:
    """
    字段配置模型。

    这个类用于描述一张表中的某一个字段在“元数据配置文件”中的定义。

    字段说明：
    - name：字段名
    - role：字段角色，例如 primary_key / foreign_key / dimension / measure
    - description：字段业务描述
    - alias：字段别名列表，用于增强自然语言召回
    - sync：是否把该字段的实际值同步到 Elasticsearch
    """
    name: str
    role: str
    description: str
    alias: list[str]
    sync: bool


@dataclass
class TableConfig:
    """
    表配置模型。

    这个类用于描述一张业务表 / 维度表的配置定义。

    字段说明：
    - name：表名
    - role：表角色，例如 dim / fact
    - description：表的业务描述
    - columns：该表包含的字段配置列表
    """
    name: str
    role: str
    description: str
    columns: list[ColumnConfig]


@dataclass
class MetricConfig:
    """
    指标配置模型。

    这个类用于描述一个业务指标的配置定义。

    字段说明：
    - name：指标名称，例如 GMV / AOV
    - description：指标的业务解释
    - relevant_columns：该指标关联的底层字段列表
    - alias：指标别名列表，用于增强自然语言召回
    """
    name: str
    description: str
    relevant_columns: list[str]
    alias: list[str]


@dataclass
class MetaConfig:
    """
    元数据总配置模型。

    这是整个 meta_config.yaml 文件对应的顶层配置对象。

    它包含两部分：
    - tables：表配置列表
    - metrics：指标配置列表
    """
    tables: list[TableConfig]
    metrics: list[MetricConfig]


# 计算配置文件路径。
#
# __file__ 表示当前 Python 文件路径。
# Path(__file__).parents[2] 的意思是：
# - 先拿到当前文件路径
# - 再向上回溯两级目录
#
# 最后拼接：
# /conf/meta_config.yaml
#
# 也就是说，这里假设项目目录结构大致类似：
# project_root/
#   conf/
#     meta_config.yaml
#   app/
#     ...
config_file = Path(__file__).parents[2] / "conf" / "meta_config.yaml"

# 创建“结构化配置 schema”
#
# OmegaConf.structured(MetaConfig) 的作用是：
# 根据 MetaConfig 这个 dataclass，生成一个“结构化配置模板”。
#
# 这个模板会包含：
# - 允许有哪些字段
# - 每个字段应该是什么类型
# - 嵌套结构应该长什么样
#
# 也就是说，这一步是在定义：
# “meta_config.yaml 应该符合 MetaConfig 这套结构”
schema = OmegaConf.structured(MetaConfig)

# 从 YAML 文件中加载实际内容。
#
# 例如 meta_config.yaml 中可能有：
# tables:
#   - name: dim_region
#     ...
# metrics:
#   - name: GMV
#     ...
#
# 加载后 content 会是一个 OmegaConf 配置对象。
content = OmegaConf.load(config_file)

# 合并 schema 和 YAML 内容，并在合并过程中进行校验。
#
# 这一步很重要，它做了两件事：
# 1. 把 YAML 文件中的实际值填充到 schema 里
# 2. 检查 YAML 内容是否符合 MetaConfig 的结构要求
#
# 例如：
# - 如果某个字段本该是 list[str]，却写成了 int
# - 或某个必填字段缺失
# 这里就可能报错
conf = OmegaConf.merge(schema, content)

# 把 OmegaConf 配置对象转换成普通 Python 对象。
#
# 因为 conf 是基于 MetaConfig 这个 dataclass schema 构造出来的，
# 所以这里最终返回的通常就是一个 MetaConfig 实例。
#
# 也就是说，meta_config 的结构大致会是：
# MetaConfig(
#     tables=[TableConfig(...), TableConfig(...)],
#     metrics=[MetricConfig(...), MetricConfig(...)]
# )
meta_config: MetaConfig = OmegaConf.to_object(conf)

# 如果直接运行当前文件，则进入这里。
if __name__ == "__main__":
    # 打印第一个指标的名字。
    #
    # 这里的写法说明：
    # - meta_config.metrics 是一个 list[MetricConfig]
    # - meta_config.metrics[0] 是第一个 MetricConfig 对象
    # - .name 是该指标的名称
    #
    # 如果配置文件中第一个指标是 GMV，
    # 那么这里会打印：
    # GMV
    print(meta_config.metrics[0].name)
    print(meta_config)
    print(conf)