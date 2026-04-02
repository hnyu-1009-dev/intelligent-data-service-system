# json：
# Python 标准库中的 JSON 处理模块。
# 这里主要用于把后端返回的 chunk 数据转换成 JSON 字符串，
# 再通过 SSE（Server-Sent Events）格式推送给前端。
import json

# APIRouter：
# FastAPI 提供的路由分组工具。
# 用于把一组接口统一挂载到某个前缀下。
#
# Depends：
# FastAPI 的依赖注入机制。
# 用于声明当前接口依赖哪些对象或函数。
from fastapi import APIRouter, Depends

# StreamingResponse：
# Starlette / FastAPI 提供的流式响应类。
# 用于把一个可迭代对象（或异步可迭代对象）的内容持续推送给客户端。
#
# 在这里，它被用来实现 SSE 流式输出，
# 让前端可以边接收边展示后端生成过程。
from starlette.responses import StreamingResponse

# get_chat_service：
# 项目中的依赖函数。
# 它通常负责返回一个 chat_service 实例，
# 这个实例应该封装了“问数 / 对话 / graph 执行”的业务逻辑。
from app.api.deps import get_chat_service

# QuerySchema：
# 项目定义的请求体数据模型。
# 一般用于校验前端传入的 query 请求内容。
#
# 例如前端可能传：
# {
#   "query": "统计一下2025年1月份各品类的销售额占比"
# }
from app.schemas.chat import QuerySchema

# 创建一个路由对象，并统一加上前缀 /api
# 也就是说，后面定义的 /query 实际完整路径会是：
# /api/query
chat_router = APIRouter(prefix="/api")


@chat_router.post("/query")
async def date_query(query: QuerySchema, chat_service=Depends(get_chat_service)):
    """
    问数流式接口。

    这个接口的核心作用是：
    1. 接收前端传入的自然语言问题
    2. 调用 chat_service.stream_chat(query.query) 执行问数流程
    3. 把后端产生的中间结果 / 最终结果以流式方式持续推送给前端

    这里使用的是 SSE（Server-Sent Events）风格的响应格式：
    每次返回一条：
        data: xxx\n\n

    前端可以使用 EventSource 或兼容 SSE 的方式持续接收数据。

    参数说明：
    - query: QuerySchema
      前端请求体，里面通常包含 query.query 这样的用户问题文本
    - chat_service:
      通过 FastAPI Depends 注入的业务服务对象
    """

    async def event_stream():
        """
        SSE 流式事件生成器。

        这个内部异步生成器会：
        1. 调用 chat_service.stream_chat(query.query)
        2. 持续接收后端产生的 chunk
        3. 把 chunk 封装成 SSE 格式输出

        如果执行过程中报错，则把错误也封装成一条 SSE 数据发给前端。
        """
        try:
            # 调用 chat_service 的流式对话方法。
            #
            # 这里的 stream_chat(query.query) 很可能内部会：
            # - 运行 LangGraph
            # - 持续产出 stage / result / error 等 chunk
            #
            # 例如 chunk 可能是：
            # {"stage": "召回字段信息"}
            # {"stage": "生成SQL"}
            # {"result": [...]}
            async for chunk in chat_service.stream_chat(query.query):
                # 把 chunk 转成 JSON 字符串，
                # 并按 SSE 协议格式封装：
                #
                # data: {"stage": "生成SQL"}
                #
                # 注意：
                # 每条消息后面必须跟 \n\n，
                # 这样前端才能正确识别一条完整的 SSE 消息。
                #
                # 参数说明：
                # - ensure_ascii=False：避免中文被转成 \uXXXX，前端更容易直接显示
                # - default=str：如果对象里有 datetime 等不能直接 JSON 序列化的类型，
                #   就自动转成字符串
                yield f"data: {json.dumps(chunk, ensure_ascii=False, default=str)}\n\n"

        except Exception as e:
            # 如果流式执行过程中抛出异常，
            # 这里不会直接让整个接口崩掉，而是把错误也作为一条 SSE 消息返回给前端。
            #
            # 这样前端就能在流式过程中直接收到：
            # {"error": "..."}
            yield f"data: {json.dumps({'error': str(e)}, ensure_ascii=False, default=str)}\n\n"

    # 返回 StreamingResponse，把 event_stream() 作为流式数据源。
    #
    # media_type="text/event-stream" 表示这是 SSE 响应类型。
    # 前端看到这个 Content-Type 后，会按事件流方式处理响应。
    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
    )