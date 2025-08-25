# -*- coding: utf-8 -*-
import pytest
import asyncio
import os
import glob
import shutil
import libtorrent as lt
import time
from screenshot.service import ScreenshotService

TEST_VIDEO_PATH = os.path.join(os.path.dirname(__file__), 'Big_Buck_Bunny_720_10s_10MB.mp4')
TEST_TORRENT_PATH = os.path.join(os.path.dirname(__file__), 'test.torrent')
TEST_OUTPUT_DIR = "./screenshots_output_test"
TEST_TIMEOUT = 120

@pytest.fixture(scope="module")
def torrent_file_info():
    """
    Creates a .torrent file and returns its path and torrent_info object.
    """
    fs = lt.file_storage()
    lt.add_files(fs, TEST_VIDEO_PATH)
    t = lt.create_torrent(fs)
    t.set_comment("screenshot integration test torrent")
    t.set_creator("pytest")
    lt.set_piece_hashes(t, os.path.dirname(TEST_VIDEO_PATH))

    with open(TEST_TORRENT_PATH, "wb") as f:
        f.write(lt.bencode(t.generate()))

    ti = lt.torrent_info(TEST_TORRENT_PATH)
    yield TEST_TORRENT_PATH, ti

    if os.path.exists(TEST_TORRENT_PATH):
        os.remove(TEST_TORRENT_PATH)

@pytest.mark.asyncio
async def test_full_screenshot_process_local(torrent_file_info):
    """
    A robust, local, end-to-end integration test.
    It seeds a local torrent and connects the downloader to it directly,
    bypassing any need for trackers or DHT, making the test stable.
    """
    torrent_path, ti = torrent_file_info
    infohash = str(ti.info_hashes().v1)

    # --- 1. Setup ---
    if os.path.exists(TEST_OUTPUT_DIR): shutil.rmtree(TEST_OUTPUT_DIR)
    os.makedirs(TEST_OUTPUT_DIR)
    torrent_data_path = f"/dev/shm/{infohash}"
    if os.path.exists(torrent_data_path): shutil.rmtree(torrent_data_path)

    seeder_ses, downloader_service = None, None
    try:
        # --- 2. Create and connect Seeder and Downloader ---
        seeder_ses = lt.session({'listen_interfaces': '127.0.0.1:6970'})
        seeder_params = {'ti': ti, 'save_path': os.path.dirname(TEST_VIDEO_PATH)}
        seeder_handle = seeder_ses.add_torrent(seeder_params)
        seeder_handle.set_flags(lt.torrent_flags.upload_mode)

        loop = asyncio.get_running_loop()
        downloader_service = ScreenshotService(loop=loop, output_dir=TEST_OUTPUT_DIR)
        await downloader_service.run()

        # Submit task via torrent file content
        with open(torrent_path, "rb") as f:
            torrent_content = f.read()
        await downloader_service.submit_task(torrent_file_content=torrent_content)

        # Give the service a moment to add the torrent
        await asyncio.sleep(1)

        # Find the torrent in the downloader service and connect it to the seeder
        downloader_handle = downloader_service.ses.find_torrent(ti.info_hashes().v1)
        assert downloader_handle.is_valid(), "Downloader could not find the added torrent."

        downloader_handle.connect_peer(("127.0.0.1", seeder_ses.listen_port()))

        # --- 3. Execute and Wait ---
        await asyncio.wait_for(downloader_service.task_queue.join(), timeout=TEST_TIMEOUT)

        # --- 4. Assert ---
        output_files = glob.glob(os.path.join(TEST_OUTPUT_DIR, f"{infohash}_*.jpg"))
        print(f"在 {TEST_OUTPUT_DIR} 中找到的截图文件: {output_files}")

        assert len(output_files) > 0, "服务未能生成任何截图文件"
        first_file = output_files[0]
        assert os.path.getsize(first_file) > 1000, "截图文件大小异常"
        with open(first_file, 'rb') as f:
            assert f.read(2) == b'\xFF\xD8', "文件不是有效的 JPG"

    finally:
        # --- 5. Cleanup ---
        if downloader_service: downloader_service.stop()
        if seeder_ses and seeder_handle.is_valid():
            seeder_ses.remove_torrent(seeder_handle)
        del seeder_ses
        if os.path.exists(TEST_OUTPUT_DIR): shutil.rmtree(TEST_OUTPUT_DIR)
        if os.path.exists(torrent_data_path): shutil.rmtree(torrent_data_path)
