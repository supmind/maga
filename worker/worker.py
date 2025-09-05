# -*- coding: utf-8 -*-
"""
本文件是截图服务的工作节点 (Worker) 的主程序。
它包含与调度器通信的客户端、回调逻辑以及运行截图服务的主循环。
"""
import sys
import os

# 将项目根目录添加到 Python 路径中，以确保可以找到 config 模块
# 和 worker 目录下的其他模块。
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import aiohttp
import uuid
import logging
import base64
import os
import signal
from functools import partial
from typing import Optional, Any, Dict

from worker.screenshot.service import ScreenshotService
from config import Settings

# --- 全局配置 ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger("Worker")

ID_FILE = ".worker_id"

def get_persistent_worker_id() -> str:
    """
    获取持久化的 Worker ID。
    如果 .worker_id 文件存在，则从中读取 ID。
    如果不存在，则生成一个新的 UUID，保存到文件中，然后返回。
    """
    if os.path.exists(ID_FILE):
        with open(ID_FILE, "r") as f:
            worker_uuid = f.read().strip()
            log.info(f"从 {ID_FILE} 文件中加载到已存在的 Worker ID: {worker_uuid}")
            return worker_uuid

    worker_uuid = str(uuid.uuid4())
    with open(ID_FILE, "w") as f:
        f.write(worker_uuid)
    log.info(f"生成了新的 Worker ID 并保存至 {ID_FILE}: {worker_uuid}")
    return worker_uuid

WORKER_ID = f"worker-{get_persistent_worker_id()}"
HEARTBEAT_INTERVAL = 30
POLL_INTERVAL = 10


class SchedulerAPIClient:
    """
    一个封装了与调度器所有 API 交互的客户端。
    这使得网络逻辑集中化，并简化了测试（通过 mock 这个类而不是网络请求）。
    """
    def __init__(self, session: aiohttp.ClientSession, scheduler_url: str, api_key: str):
        self._session = session
        self._url = scheduler_url
        self._api_key = api_key

    def _get_headers(self) -> Dict[str, str]:
        """构造带有认证信息的请求头。"""
        return {"X-API-Key": self._api_key}

    async def register(self, worker_id: str) -> bool:
        """向调度器注册当前工作节点。"""
        url = f"{self._url}/workers/register"
        payload = {"worker_id": worker_id, "status": "idle"}
        try:
            async with self._session.post(url, json=payload, headers=self._get_headers()) as response:
                if response.status == 200:
                    log.info(f"工作节点 {worker_id} 注册成功。")
                    return True
                log.error(f"注册工作节点失败。状态码: {response.status}, 响应: {await response.text()}")
                return False
        except aiohttp.ClientError as e:
            log.error(f"注册工作节点时发生连接错误: {e}")
            return False

    async def get_next_task(self, worker_id: str) -> Optional[Dict[str, Any]]:
        """向调度器请求下一个待处理的任务。"""
        url = f"{self._url}/tasks/next?worker_id={worker_id}"
        try:
            async with self._session.get(url, timeout=15, headers=self._get_headers()) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 204:
                    log.info("调度器中无可用任务，等待中...")
                    return None
        except aiohttp.ClientError as e:
            log.error(f"连接调度器获取任务时出错: {e}")
        return None

    async def record_screenshot(self, infohash: str, filename: str):
        """向调度器报告一个截图已成功生成并上传。"""
        log.info(f"[{infohash}] 正在向调度器报告截图: {filename}")
        url = f"{self._url}/tasks/{infohash}/screenshots"
        payload = {"filename": filename}
        try:
            async with self._session.post(url, json=payload, headers=self._get_headers()) as response:
                if response.status != 200:
                    log.error(f"[{infohash}] 报告截图失败。状态码: {response.status}, 响应: {await response.text()}")
        except aiohttp.ClientError as e:
            log.error(f"[{infohash}] 报告截图时发生连接错误: {e}")

    async def update_task_status(self, infohash: str, status: str, message: str, resume_data: Optional[dict]):
        """向调度器报告任务的最终状态。"""
        log.info(f"[{infohash}] 任务完成，状态: {status.upper()}。消息: {message}")
        url = f"{self._url}/tasks/{infohash}/status"

        if resume_data:
            if extractor_info := resume_data.get("extractor_info"):
                if extradata := extractor_info.get("extradata"):
                    if isinstance(extradata, bytes):
                        extractor_info["extradata"] = base64.b64encode(extradata).decode('ascii')

        payload = {"status": status, "message": str(message), "resume_data": resume_data}
        try:
            async with self._session.post(url, json=payload, headers=self._get_headers()) as response:
                if response.status != 200:
                    log.error(f"[{infohash}] 报告最终状态失败。状态码: {response.status}, 响应: {await response.text()}")
        except aiohttp.ClientError as e:
            log.error(f"[{infohash}] 报告最终状态时发生连接错误: {e}")

    async def update_task_details(self, infohash: str, details: dict):
        """向调度器报告任务的元数据详情。"""
        log.info(f"[{infohash}] 正在上报任务详情: {details}")
        url = f"{self._url}/tasks/{infohash}/details"
        try:
            async with self._session.post(url, json=details, headers=self._get_headers()) as response:
                if response.status != 200:
                    log.error(f"[{infohash}] 上报任务详情失败。状态码: {response.status}, 响应: {await response.text()}")
        except aiohttp.ClientError as e:
            log.error(f"[{infohash}] 上报任务详情时发生连接错误: {e}")

    async def send_heartbeat(self, worker_id: str, service: ScreenshotService):
        """定期发送心跳以保持工作节点活动状态。"""
        url = f"{self._url}/workers/heartbeat"
        status = "busy" if service.active_tasks else "idle"

        # 从 ScreenshotService 实例动态获取队列大小和活动任务数
        queue_size = service.get_queue_size()
        # 正在执行的任务数 = 总任务数 - 队列中的任务数
        active_tasks_count = len(service.active_tasks) - queue_size

        payload = {
            "worker_id": worker_id,
            "status": status,
            "active_tasks_count": active_tasks_count,
            "queue_size": queue_size,
        }
        try:
            async with self._session.post(url, json=payload, headers=self._get_headers()) as response:
                if response.status != 200:
                    log.warning(f"发送心跳失败。状态码: {response.status}")
        except aiohttp.ClientError as e:
            log.warning(f"发送心跳时发生连接错误: {e}")


# --- R2 Uploader ---
try:
    import boto3
    from botocore.exceptions import ClientError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False


class R2Uploader:
    """
    一个封装了向 Cloudflare R2 上传逻辑的类。
    """
    def __init__(self, settings: Settings):
        if not BOTO3_AVAILABLE:
            log.warning("boto3 未安装，R2 上传功能将被禁用。")
            self.client = None
            return

        if not all([settings.r2_endpoint_url, settings.r2_access_key_id, settings.r2_secret_access_key, settings.r2_bucket_name]):
            log.warning("R2 配置不完整，上传功能将被禁用。请检查 .env 文件。")
            self.client = None
            return

        self.client = boto3.client(
            's3',
            endpoint_url=settings.r2_endpoint_url,
            aws_access_key_id=settings.r2_access_key_id,
            aws_secret_access_key=settings.r2_secret_access_key,
            region_name="auto" # R2 通常使用 "auto"
        )
        self.bucket_name = settings.r2_bucket_name
        log.info(f"R2 Uploader 已初始化，目标存储桶: {self.bucket_name}")

    def upload(self, data: bytes, object_name: str) -> bool:
        """
        将字节数据上传到 R2 存储桶。

        :param data: 要上传的字节数据。
        :param object_name: 在存储桶中的对象名称 (包含路径)。
        :return: 上传成功返回 True，否则返回 False。
        """
        if not self.client:
            log.error("R2 客户端未初始化，上传操作中止。")
            return False
        try:
            self.client.put_object(Bucket=self.bucket_name, Key=object_name, Body=data, ContentType='image/jpeg')
            log.info(f"成功上传 {object_name} 到 R2 存储桶 {self.bucket_name}")
        except ClientError as e:
            log.error(f"上传到 R2 时发生错误: {e}")
            return False
        return True


# --- 回调函数定义 ---

async def on_screenshot_generated(uploader: R2Uploader, client: SchedulerAPIClient, infohash: str, image_bytes: bytes, timestamp_str: str):
    """
    当 ScreenshotService 成功生成一个截图的字节数据时被调用的回调函数。
    它负责将截图上传到 R2，并在成功后通知调度器。
    """
    filename = f"{infohash}_{timestamp_str}.jpg"
    object_name = f"{infohash}/{filename}"

    # 在事件循环的默认执行器中运行阻塞的上传操作
    loop = asyncio.get_running_loop()
    success = await loop.run_in_executor(None, uploader.upload, image_bytes, object_name)

    if success:
        await client.record_screenshot(infohash, filename)

async def on_task_finished(client: SchedulerAPIClient, status: str, infohash: str, message: str, **kwargs):
    """当 ScreenshotService 完成一个任务时被调用的回调函数。"""
    await client.update_task_status(infohash, status, message, kwargs.get("resume_data"))


async def on_task_details_extracted(client: SchedulerAPIClient, infohash: str, details: dict):
    """当 ScreenshotService 提取出任务元数据时被调用的回调函数。"""
    await client.update_task_details(infohash, details)


# --- 主程序逻辑 ---

async def heartbeat_loop(
    stop_event: asyncio.Event,
    client: SchedulerAPIClient,
    service: ScreenshotService
):
    """一个独立的协程，定期向调度器发送心跳。"""
    while not stop_event.is_set():
        await client.send_heartbeat(WORKER_ID, service)
        try:
            await asyncio.wait_for(asyncio.shield(stop_event.wait()), timeout=HEARTBEAT_INTERVAL)
        except asyncio.TimeoutError:
            pass

async def main_loop(
    stop_event: asyncio.Event,
    client: SchedulerAPIClient,
    service: ScreenshotService,
    settings: Settings
):
    """工作节点的主轮询循环，负责获取和提交任务。"""
    while not stop_event.is_set():
        try:
            if service.get_queue_size() >= settings.worker_max_queue_size:
                await asyncio.sleep(POLL_INTERVAL)
                continue

            task_data = await client.get_next_task(WORKER_ID)
            if task_data:
                log.info(f"收到新任务: {task_data['infohash']}，提交到本地服务。")
                if metadata := task_data.get('metadata'):
                    task_data['metadata'] = base64.b64decode(metadata)
                await service.submit_task(**task_data)
                await asyncio.sleep(0.1)
            else:
                log.info("调度器中无更多可用任务，进入等待状态。")
                await asyncio.sleep(POLL_INTERVAL)
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.error(f"主轮询循环发生意外错误: {e}", exc_info=True)
            await asyncio.sleep(POLL_INTERVAL)

async def run_worker(session: aiohttp.ClientSession):
    """设置并运行工作节点的所有组件。"""
    settings = Settings()
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _handle_signal():
        log.info("接收到停机信号...")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    client = SchedulerAPIClient(session, settings.scheduler_url, settings.scheduler_api_key)
    if not await client.register(WORKER_ID):
        log.error("无法向调度器注册，程序退出。")
        return

    uploader = R2Uploader(settings)

    service = ScreenshotService(
        settings=settings,
        loop=loop,
        status_callback=partial(on_task_finished, client),
        screenshot_callback=partial(on_screenshot_generated, uploader, client),
        details_callback=partial(on_task_details_extracted, client)
    )
    await service.run()
    log.info("ScreenshotService 已在后台运行。")

    heartbeat = asyncio.create_task(heartbeat_loop(stop_event, client, service))
    log.info("心跳任务已启动。")

    log.info("启动主任务轮询循环...")
    await main_loop(stop_event, client, service, settings)

    log.info("正在停止服务...")
    heartbeat.cancel()
    await service.stop()
    log.info("工作节点已成功关闭。")


if __name__ == "__main__":
    async def run():
        async with aiohttp.ClientSession() as session:
            await run_worker(session)

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("程序被手动中断。")
