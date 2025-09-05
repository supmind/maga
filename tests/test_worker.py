# -*- coding: utf-8 -*-
"""
对 worker.py 的单元测试。
"""
import asyncio
from contextlib import suppress
from unittest.mock import patch, AsyncMock, MagicMock, PropertyMock, ANY

import pytest
from aiohttp import ClientSession

from worker import worker
from config import Settings
from worker.screenshot.service import ScreenshotService


@pytest.fixture
def mock_settings(tmp_path):
    """为测试提供一个 Settings 实例，并设置一个较小的队列上限。"""
    return Settings(
        scheduler_url="http://test-scheduler",
        output_dir=str(tmp_path),
        worker_max_queue_size=5
    )

@pytest.fixture
def mock_client():
    """提供一个 SchedulerAPIClient 的 AsyncMock 实例。"""
    client = AsyncMock(spec=worker.SchedulerAPIClient)
    client.get_next_task.side_effect = [None]  # 默认不返回任务
    return client

@pytest.fixture
def mock_service():
    """提供一个 ScreenshotService 的 AsyncMock 实例。"""
    service = AsyncMock(spec=ScreenshotService)
    service.get_queue_size.return_value = 0
    type(service).active_tasks = PropertyMock(return_value=set())
    return service


@pytest.mark.asyncio
async def test_main_loop_fetches_task_when_queue_not_full(mock_settings, mock_client, mock_service):
    """
    测试：当服务队列大小低于上限时，主循环会获取一个新任务。
    """
    # --- 安排 ---
    stop_event = asyncio.Event()
    mock_service.get_queue_size.return_value = mock_settings.worker_max_queue_size - 1
    mock_client.get_next_task.side_effect = [{"infohash": "test_hash"}, stop_event.set]

    # --- 执行 ---
    await worker.main_loop(stop_event, mock_client, mock_service, mock_settings)

    # --- 断言 ---
    mock_service.get_queue_size.assert_called()
    mock_client.get_next_task.assert_called()
    mock_service.submit_task.assert_awaited_once_with(infohash="test_hash")

@pytest.mark.asyncio
@patch('asyncio.sleep', new_callable=AsyncMock)
async def test_main_loop_waits_when_queue_is_full(mock_sleep, mock_settings, mock_client, mock_service):
    """
    测试：当服务队列大小达到上限时，主循环会等待。
    """
    # --- 安排 ---
    stop_event = asyncio.Event()
    mock_service.get_queue_size.return_value = mock_settings.worker_max_queue_size
    # 让 sleep 调用来停止循环，以验证 sleep 被调用了
    mock_sleep.side_effect = stop_event.set

    # --- 执行 ---
    await worker.main_loop(stop_event, mock_client, mock_service, mock_settings)

    # --- 断言 ---
    mock_service.get_queue_size.assert_called()
    mock_client.get_next_task.assert_not_awaited()
    mock_sleep.assert_awaited_once_with(worker.POLL_INTERVAL)

@pytest.mark.asyncio
async def test_on_task_finished_callback():
    """
    测试：on_task_finished 回调函数能正确调用客户端的 update_task_status 方法。
    """
    # --- 安排 ---
    mock_client = AsyncMock(spec=worker.SchedulerAPIClient)
    infohash, status, message = "test_hash", "success", "All good"
    resume_data = {"key": "value"}

    # --- 执行 ---
    await worker.on_task_finished(mock_client, status, infohash, message, resume_data=resume_data)

    # --- 断言 ---
    mock_client.update_task_status.assert_awaited_once_with(infohash, status, message, resume_data)

@pytest.mark.asyncio
async def test_scheduler_api_client_send_heartbeat():
    """
    测试 SchedulerAPIClient.send_heartbeat 方法能构建并发送正确的负载。
    """
    # --- 安排 ---
    mock_session = AsyncMock(spec=ClientSession)
    # The client now requires the api_key
    client = worker.SchedulerAPIClient(mock_session, "http://fake-scheduler", "test-key")

    mock_service = MagicMock(spec=ScreenshotService)
    type(mock_service).active_tasks = PropertyMock(return_value={"t1", "t2", "t3"})
    mock_service.get_queue_size.return_value = 1

    # --- 执行 ---
    await client.send_heartbeat("worker-id", mock_service)

    # --- 断言 ---
    expected_payload = {
        "worker_id": "worker-id",
        "status": "busy",
        "active_tasks_count": 2, # 3 (total) - 1 (queued) = 2 (processing)
        "queue_size": 1
    }
    mock_session.post.assert_awaited_once_with(
        "http://fake-scheduler/workers/heartbeat",
        json=expected_payload,
        headers={"X-API-Key": "test-key"}
    )
