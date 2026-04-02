# Path：
# 来自 pathlib 标准库，用于表示文件路径。
# 比起直接使用字符串路径，Path 更清晰、更安全，也更适合跨平台开发。
#
# 例如：
# Path("config/meta.yaml")
#
# 在这段代码里，config_file 的类型就是 Path，
# 表示传入的是一个配置文件路径对象。
from pathlib import Path

# Type / TypeVar / Union：
# 这几个都来自 typing 模块，用于类型标注。
#
# 其中：
# - Type：表示“一个类本身”，而不是类实例
# - TypeVar：定义泛型类型变量
# - Union：表示“可能是多种类型之一”
#
# 这段代码里真正核心在用的是：
# - Type[T]
# - TypeVar("T")
#
# 它们配合使用后，load_config() 就可以表达：
# “传进来什么 schema 类，就返回那个类型对应的配置对象”
from typing import Type, TypeVar, Union

# OmegaConf / DictConfig：
# OmegaConf 是 Python 中常用的配置管理库，常用于：
# - YAML 配置文件读取
# - 配置合并
# - 配置校验
# - 与 dataclass 结合做结构化配置
#
# 这里导入了：
# - OmegaConf：核心配置操作入口
# - DictConfig：OmegaConf 中的一种配置对象类型
#
# 注意：
# 当前这段代码里 DictConfig 实际没有被使用，
# 所以严格来说它是一个“多余导入”，可以删除。
from omegaconf import OmegaConf, DictConfig

# 定义一个泛型类型变量 T。
#
# 作用是：
# 让 load_config() 这个函数在类型层面具备“通用性”。
#
# 例如：
# 如果你传入的是 MetaConfig 类，
# 那么返回值类型就会被推断成 MetaConfig。
#
# 如果你传入的是 AppConfig 类，
# 那么返回值类型就会被推断成 AppConfig。
T = TypeVar("T")


def load_config(schema_cls: Type[T], config_file: Path) -> T:
    """
    加载配置文件，并按照给定的 schema 类进行结构化校验，最后返回对应的 dataclass 对象。

    参数：
        schema_cls:
            配置的结构定义类，通常是 dataclass。
            它决定了配置文件中允许有哪些字段、字段类型是什么、默认值是什么。

        config_file:
            配置文件路径，通常是 yaml 文件路径。

    返回值：
        返回一个 schema_cls 对应类型的配置对象。

    这个函数整体做了 4 件事：
    1. 根据 schema 类创建一个“结构化配置模板”
    2. 从文件中读取实际配置内容
    3. 把“模板”和“配置文件内容”合并，并在合并过程中完成校验
    4. 把最终配置转换成普通 Python 对象（通常是 dataclass 实例）
    """

    # 1. 创建 schema（用于校验）
    #
    # OmegaConf.structured(schema_cls) 的作用是：
    # 根据传入的 schema 类创建一个“结构化配置对象”。
    #
    # 这个 schema 通常来自 dataclass，里面会定义：
    # - 配置字段名称
    # - 字段类型
    # - 默认值
    #
    # 为什么这一步很重要？
    # 因为它相当于给配置文件定义了一份“规则”或“模板”。
    # 后面加载进来的内容会按照这个模板进行约束和校验。
    #
    # 例如：
    # @dataclass
    # class AppConfig:
    #     host: str = "127.0.0.1"
    #     port: int = 8080
    #
    # 那么 schema 就会描述：
    # - host 必须是 str
    # - port 必须是 int
    # - 如果配置文件没写，可以使用默认值
    schema = OmegaConf.structured(schema_cls)

    # 2. 加载内容
    #
    # OmegaConf.load(config_file) 会从文件中读取配置内容，
    # 一般支持 yaml / yml 等格式。
    #
    # 例如 config_file 内容可能是：
    # host: "0.0.0.0"
    # port: 9000
    #
    # 读取后会得到一个 OmegaConf 配置对象。
    content = OmegaConf.load(config_file)

    # 3. 合并 + 校验
    #
    # OmegaConf.merge(schema, content) 的作用非常关键：
    #
    # 它会把：
    # - schema：结构化模板
    # - content：配置文件实际内容
    # 合并成一个最终配置对象 conf。
    #
    # 这个过程不只是“覆盖默认值”，还会顺便做类型和字段校验。
    #
    # 例如：
    # - schema 里要求 port 是 int
    # - 如果配置文件写成 port: "abc"
    #   那么这里可能就会报错
    #
    # 再例如：
    # - schema 里有默认值
    # - 配置文件没写某个字段
    #   那么就会保留 schema 中的默认值
    #
    # 所以这一步本质上完成了两件事：
    # 1. 配置合并
    # 2. 配置合法性校验
    conf = OmegaConf.merge(schema, content)

    # 4. 转为 dataclass 对象
    #
    # OmegaConf.to_object(conf) 会把 OmegaConf 的配置对象，
    # 转换成普通 Python 对象。
    #
    # 在这里，因为 conf 是基于结构化 schema 构建出来的，
    # 所以最终返回的通常就是 schema_cls 对应的 dataclass 实例。
    #
    # 这样做的好处是：
    # 1. 业务代码拿到的是普通对象，不需要到处处理 OmegaConf 特有类型
    # 2. 可以直接通过点语法访问字段，例如：
    #    config.host
    #    config.port
    # 3. 类型提示更友好，IDE 自动补全体验更好
    return OmegaConf.to_object(conf)
