import sys
import uuid
from pathlib import Path

from loguru import logger

from app.config.app_config import app_config
from app.core.context import request_id_ctx_var

# 定义日志输出格式。
# loguru 支持通过占位符把日志的各个字段拼接成自定义格式。
#
# 这里的格式含义如下：
# 1. {time:YYYY-MM-DD HH:mm:ss.SSS}
#    - 日志打印时间，精确到毫秒
# 2. {level: <8}
#    - 日志级别，左对齐并固定宽度 8 个字符，便于输出整齐
# 3. {extra[request_id]}
#    - 从 extra 字段中读取 request_id，用于链路追踪
# 4. {name}:{function}:{line}
#    - 当前日志所在模块名、函数名、代码行号
# 5. {message}
#    - 真正的日志内容
#
# <green>、<level>、<magenta>、<cyan> 是 loguru 提供的颜色标签，
# 主要用于控制台高亮显示，便于开发时阅读。
log_format = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<magenta>request_id - {extra[request_id]}</magenta> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
    "<level>{message}</level>"
)


def inject_request_id(record):
    # 这是一个“日志补丁函数”，会在每条日志真正输出前被调用。
    # 它的作用是：往日志记录 record 中动态注入 request_id，
    # 这样后面的日志格式里就可以安全地使用 {extra[request_id]}。
    #
    # record 是 loguru 维护的一条日志记录，底层本质上是一个字典，
    # 里面包含 time、level、message、extra 等信息。
    try:
        # 从上下文变量 request_id_ctx_var 中获取当前请求的 request_id。
        #
        # 这种做法常用于 Web 服务、异步服务或中间件中：
        # 每个请求进入系统时生成一个唯一 request_id，
        # 然后在整个调用链路里复用这个值，
        # 这样就能把同一个请求触发的所有日志串起来。
        request_id = request_id_ctx_var.get()
    except Exception as e:
        # 如果当前上下文里没有 request_id，
        # 就生成一个随机 UUID 作为兜底值。
        #
        # 常见场景包括：
        # 1. 当前日志不是在 HTTP 请求上下文中打印的
        # 2. 程序启动阶段、脚本模式下执行
        # 3. 某些异步上下文没有正确传递 request_id
        #
        # 这里使用 uuid.uuid4() 生成一个随机全局唯一标识。
        request_id = uuid.uuid4()

    # 把 request_id 注入到 record["extra"] 里。
    # 这样 log_format 中的 {extra[request_id]} 才能正常取到值。
    record["extra"]["request_id"] = request_id


# 移除 loguru 默认自带的日志处理器。
# 如果不先 remove()，loguru 默认会往控制台输出一份日志，
# 你后面再 add() 自己的 sink 时，可能会出现重复打印。
logger.remove()

# 使用 patch() 给 logger 打一个“补丁”。
# 打补丁后的 logger 在每次记录日志前，都会先执行 inject_request_id(record)。
# 这样我们就实现了“自动给每条日志附加 request_id”的效果。
logger = logger.patch(inject_request_id)

# 如果配置里启用了控制台日志输出，就添加一个输出到标准输出流（stdout）的日志 sink。
if app_config.logging.console.enable:
    logger.add(
        sink=sys.stdout,
        level=app_config.logging.console.level,
        format=log_format
    )

# 如果配置里启用了文件日志输出，就添加一个输出到文件的日志 sink。
if app_config.logging.file.enable:
    # 从配置中读取日志目录路径，并构造 Path 对象。
    path = Path(app_config.logging.file.path)

    # 创建日志目录。
    # parents=True 表示如果父目录不存在，就一并创建。
    # exist_ok=True 表示如果目录已经存在，不报错。
    path.mkdir(parents=True, exist_ok=True)

    logger.add(
        # 指定日志文件路径。
        # 最终会写入到类似：logs/app.log
        sink=path / "app.log",

        # 设置该 sink 接收的最低日志级别，例如 INFO、DEBUG、ERROR。
        level=app_config.logging.file.level,

        # 指定日志输出格式，和控制台保持一致。
        format=log_format,

        # 日志轮转策略。
        # 例如可以配置成 "500 MB"、"1 day"，
        # 表示文件达到指定大小或时间后自动切分新文件。
        rotation=app_config.logging.file.rotation,

        # 日志保留策略。
        # 例如可以配置成 "7 days"，
        # 表示只保留最近 7 天的日志，旧日志自动删除。
        retention=app_config.logging.file.retention,

        # 指定日志文件编码，避免中文乱码。
        encoding="utf-8"
    )


if __name__ == '__main__':
    # 当前文件直接运行时，输出一条测试日志。
    #
    # 这样可以快速验证：
    # 1. logger 是否初始化成功
    # 2. 控制台日志是否正常输出
    # 3. 文件日志是否正常写入
    # 4. request_id 是否被正确注入
    logger.info("hello world")