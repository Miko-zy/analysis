# start_data_analysis_mcp.py
"""
MCP stdio <-> WebSocket pipe for 数据分析工具
"""

import asyncio
import websockets
import subprocess
import logging
import os
import signal
import sys
import json

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('DataAnalysisMCP')

# 重新连接设置
INITIAL_BACKOFF = 1
MAX_BACKOFF = 600


async def connect_with_retry(uri):
    """带重试机制连接WebSocket服务器"""
    reconnect_attempt = 0
    backoff = INITIAL_BACKOFF
    while True:
        try:
            if reconnect_attempt > 0:
                logger.info(f"等待 {backoff}s 后进行第 {reconnect_attempt} 次重连...")
                await asyncio.sleep(backoff)

            await connect_to_server(uri)

        except Exception as e:
            reconnect_attempt += 1
            logger.warning(f"连接关闭 (尝试 {reconnect_attempt}): {e}")
            backoff = min(backoff * 2, MAX_BACKOFF)


async def connect_to_server(uri):
    """连接WebSocket服务器并建立stdio管道"""
    try:
        logger.info("连接到WebSocket服务器...")
        async with websockets.connect(uri) as websocket:
            logger.info("成功连接到WebSocket服务器")

            # 启动数据分析工具进程
            process = subprocess.Popen(
                [sys.executable, "data_analysis_tool.py"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                encoding='utf-8',
                text=True
            )
            logger.info("已启动数据分析工具进程")

            # 创建两个任务：WebSocket到进程，进程到WebSocket
            await asyncio.gather(
                pipe_websocket_to_process(websocket, process),
                pipe_process_to_websocket(process, websocket),
                pipe_process_stderr_to_terminal(process)
            )
    except websockets.exceptions.ConnectionClosed as e:
        logger.error(f"WebSocket连接关闭: {e}")
        raise
    except Exception as e:
        logger.error(f"连接错误: {e}")
        raise
    finally:
        if 'process' in locals():
            logger.info("终止服务器进程")
            try:
                process.terminate()
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
            logger.info("服务器进程已终止")


async def pipe_websocket_to_process(websocket, process):
    """从WebSocket读取数据并写入进程stdin"""
    try:
        while True:
            message = await websocket.recv()
            logger.debug(f"<< {message[:120]}...")

            if isinstance(message, bytes):
                message = message.decode('utf-8')
            process.stdin.write(message + '\n')
            process.stdin.flush()
    except Exception as e:
        logger.error(f"WebSocket到进程管道错误: {e}")
        raise
    finally:
        if not process.stdin.closed:
            process.stdin.close()


async def pipe_process_to_websocket(process, websocket):
    """从进程stdout读取数据并发送到WebSocket"""
    try:
        while True:
            data = await asyncio.to_thread(process.stdout.readline)

            if not data:
                logger.info("进程已结束输出")
                break

            logger.debug(f">> {data[:120]}...")
            await websocket.send(data)
    except Exception as e:
        logger.error(f"进程到WebSocket管道错误: {e}")
        raise


async def pipe_process_stderr_to_terminal(process):
    """从进程stderr读取数据并打印到终端"""
    try:
        while True:
            data = await asyncio.to_thread(process.stderr.readline)

            if not data:
                logger.info("进程stderr已结束输出")
                break

            sys.stderr.write(data)
            sys.stderr.flush()
    except Exception as e:
        logger.error(f"进程stderr管道错误: {e}")
        raise


def signal_handler(sig, frame):
    """处理中断信号"""
    logger.info("收到中断信号，正在关闭...")
    sys.exit(0)


if __name__ == "__main__":
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)

    # 从环境变量获取端点URL
    endpoint_url = os.environ.get('MCP_ENDPOINT')
    if not endpoint_url:
        logger.error("请设置 `MCP_ENDPOINT` 环境变量")
        sys.exit(1)

    try:
        asyncio.run(connect_with_retry(endpoint_url))
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
    except Exception as e:
        logger.error(f"程序执行错误: {e}")