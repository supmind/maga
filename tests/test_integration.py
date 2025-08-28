# -*- coding: utf-8 -*-
import pytest
import asyncio
import os
import shutil
import glob
import pytest_asyncio
from unittest.mock import patch

from screenshot.service import (
    ScreenshotService,
    AllSuccessResult,
    FatalErrorResult,
    PartialSuccessResult,
)

# A known torrent with a video file that is well-seeded
SINTEL_INFOHASH = "08ada5a7a6183aae1e09d831df6748d566095a10"
# A known torrent that reliably times out on metadata fetch in this environment
METADATA_TIMEOUT_INFOHASH = "d5d2c854c3563914a1f6a4574996969542b8813b"

TEST_OUTPUT_DIR = "./screenshots_output_test"
TEST_DATA_DIR = "./torrent_data_test"
NETWORK_TEST_TIMEOUT = 200  # Must be > internal client timeouts

@pytest_asyncio.fixture
async def service_setup():
    """Fixture to set up and tear down the service and directories for each test."""
    if os.path.exists(TEST_OUTPUT_DIR):
        shutil.rmtree(TEST_OUTPUT_DIR)
    if os.path.exists(TEST_DATA_DIR):
        shutil.rmtree(TEST_DATA_DIR)
    os.makedirs(TEST_OUTPUT_DIR)
    os.makedirs(TEST_DATA_DIR)

    service = ScreenshotService(
        output_dir=TEST_OUTPUT_DIR,
        torrent_save_path=TEST_DATA_DIR
    )
    await service.run()

    yield service

    service.stop()
    await asyncio.sleep(0.1)
    if os.path.exists(TEST_OUTPUT_DIR):
        shutil.rmtree(TEST_OUTPUT_DIR)
    if os.path.exists(TEST_DATA_DIR):
        shutil.rmtree(TEST_DATA_DIR)

@pytest.mark.asyncio
class TestIntegration:
    """A suite of integration tests for the ScreenshotService."""

    async def run_task(self, service, infohash, resume_data=None):
        """Helper to run a task and wait for its result."""
        result_event = asyncio.Event()
        result_storage = {}
        def on_complete(result):
            result_storage['result'] = result
            result_event.set()

        await service.submit_task(infohash=infohash, on_complete=on_complete, resume_data=resume_data)
        await asyncio.wait_for(result_event.wait(), timeout=NETWORK_TEST_TIMEOUT)
        return result_storage['result']

    async def test_full_success_scenario(self, service_setup):
        """Tests the end-to-end success case."""
        result = await self.run_task(service_setup, SINTEL_INFOHASH)

        assert isinstance(result, AllSuccessResult)
        assert result.screenshots_count > 0
        output_files = glob.glob(os.path.join(TEST_OUTPUT_DIR, f"{SINTEL_INFOHASH}_*.jpg"))
        assert len(output_files) == result.screenshots_count

    async def test_fatal_error_on_metadata_timeout(self, service_setup):
        """Tests a fatal error when metadata fetch times out."""
        result = await self.run_task(service_setup, METADATA_TIMEOUT_INFOHASH)

        assert isinstance(result, FatalErrorResult)
        assert "获取元数据超时" in result.reason

    async def test_partial_failure_on_moov_timeout(self, service_setup):
        """Tests a recoverable failure when moov download times out."""
        service = service_setup
        service.MOOV_TIMEOUT_FOR_TESTING = 0.01

        result = await self.run_task(service, SINTEL_INFOHASH)

        assert isinstance(result, PartialSuccessResult)
        assert "获取 moov atom 超时或出错" in result.reason
        assert result.resume_data == {}

    async def test_multi_stage_resume_from_partial_success(self, service_setup):
        """
        Tests the cumulative resume logic by deterministically failing the task.
        """
        service = service_setup

        # --- Run 1: Fail after processing exactly 1 keyframe ---
        service.FAIL_AFTER_N_KEYFRAMES = 1
        result1 = await self.run_task(service, SINTEL_INFOHASH)

        assert isinstance(result1, PartialSuccessResult)
        assert result1.screenshots_count == 1
        assert "等待 pieces 超时" in result1.reason
        assert "moov_data_b64" in result1.resume_data

        processed_run1 = set(result1.resume_data['processed_kf_indices'])
        assert len(processed_run1) == 1

        # --- Run 2: Fail again after processing 2 more keyframes ---
        service.FAIL_AFTER_N_KEYFRAMES = 2
        result2 = await self.run_task(service, SINTEL_INFOHASH, result1.resume_data)

        assert isinstance(result2, PartialSuccessResult)
        assert result2.screenshots_count == 2

        processed_run2 = set(result2.resume_data['processed_kf_indices'])
        assert processed_run2.issuperset(processed_run1)
        assert len(processed_run2) == len(processed_run1) + result2.screenshots_count
        assert len(processed_run2) == 3 # 1 from run 1, 2 from run 2

        # --- Run 3: Succeed with the second resume_data ---
        del service.FAIL_AFTER_N_KEYFRAMES
        result3 = await self.run_task(service, SINTEL_INFOHASH, result2.resume_data)

        assert isinstance(result3, AllSuccessResult)

        # --- Final verification ---
        total_screenshots = result1.screenshots_count + result2.screenshots_count + result3.screenshots_count
        all_kf_in_resume_data = len(result2.resume_data['all_kf_indices'])

        assert total_screenshots == all_kf_in_resume_data

        output_files = glob.glob(os.path.join(TEST_OUTPUT_DIR, f"{SINTEL_INFOHASH}_*.jpg"))
        assert len(output_files) == total_screenshots

    async def test_fatal_error_on_corrupt_mp4(self, service_setup):
        """
        Tests a fatal error when the mp4 file is corrupt and cannot be parsed.
        This is simulated by patching the H264KeyframeExtractor to raise an exception.
        """
        service = service_setup

        # We patch the extractor in the service module where it's used.
        with patch('screenshot.service.H264KeyframeExtractor', side_effect=Exception("模拟解析失败")):
            result = await self.run_task(service, SINTEL_INFOHASH)

        assert isinstance(result, FatalErrorResult)
        assert "初始化任务失败: 模拟解析失败" in result.reason
