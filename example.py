import asyncio
import signal
import binascii
import logging
import os

import bencode2 as bencoder
from maga.crawler import Maga
from maga.downloader import Downloader

# # 配置日志记录器 (注释掉以减少输出)
# logging.basicConfig(
#     level=logging.DEBUG,
#     format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
#     datefmt="%Y-%m-%d %H:%M:%S",
# )

# 使用一个集合（set）来记录已经提交给下载器的infohash
# 这是为了防止爬虫因为频繁收到同一个infohash的announce消息而重复提交任务
SUBMITTED_INFOHASHES = set()


def format_bytes(size):
    """将字节大小格式化为可读的字符串（KB, MB, GB等）"""
    if size is None:
        return "N/A"
    power = 1024
    n = 0
    power_labels = {0: '', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    while size > power and n < len(power_labels) -1 :
        size /= power
        n += 1
    return f"{size:.2f} {power_labels[n]}B"


async def process_results(downloader):
    """
    一个辅助函数，用于处理下载成果
    """
    # 确保保存.torrent文件的目录存在
    os.makedirs("torrents", exist_ok=True)

    while True:
        # 从成果队列中获取一个结果
        infohash, metadata = await downloader.results_queue.get()
        infohash_hex = binascii.hexlify(infohash).decode()

        # 将metadata字典进行bencode编码
        torrent_data = bencoder.bencode(metadata)
        file_path = os.path.join("torrents", f"{infohash_hex}.torrent")

        try:
            # 将bencode编码后的数据写入.torrent文件
            with open(file_path, "wb") as f:
                f.write(torrent_data)

            # 提取核心信息
            info = metadata.get(b'info', {})
            name = info.get(b'name', b'Unknown').decode(errors='ignore')

            if b'files' in info:
                # 多文件模式
                num_files = len(info[b'files'])
                total_size = sum(f[b'length'] for f in info[b'files'])
            else:
                # 单文件模式
                num_files = 1
                total_size = info.get(b'length')

            # 按用户要求，只在成功后打印元数据摘要
            print("="*30 + " 下载成功 " + "="*30)
            print(f"  Infohash: {infohash_hex}")
            print(f"  文件名: {name}")
            print(f"  文件数: {num_files}")
            print(f"  总大小: {format_bytes(total_size)}")
            print(f"  已保存到: {file_path}")
            print("="*70 + "\n")

        except Exception:
            # 隐藏保存时的错误信息，保持安静
            pass
        finally:
            # 通知队列这项任务已完成
            downloader.results_queue.task_done()


# async def print_dht_stats(downloader):
#     ... (注释掉)


async def main():
    # 获取当前的asyncio事件循环
    loop = asyncio.get_running_loop()

    # 1. 创建下载器（Downloader）服务实例
    downloader = Downloader(loop=loop, dht_port=6882, num_workers=10)

    # 2. 为爬虫（Crawler）定义回调函数（handler）
    async def crawler_handler(infohash, peer_addr):
        infohash_hex = binascii.hexlify(infohash).decode()
        if infohash_hex not in SUBMITTED_INFOHASHES:
            SUBMITTED_INFOHASHES.add(infohash_hex)
            await downloader.submit(infohash)

    # 3. 创建爬虫（Crawler）服务实例
    crawler = Maga(loop=loop, handler=crawler_handler)

    # 4. 启动两个服务
    await downloader.run()
    await crawler.run(port=6881)

    # 启动后台任务
    results_task = loop.create_task(process_results(downloader))

    print("服务已启动，正在后台爬取和下载...")
    print("成功下载的元数据内容将被格式化打印出来。")
    print("按 Ctrl+C 停止运行。")

    # 5. 等待程序被中断 (Ctrl+C)
    stop = asyncio.Future()
    loop.add_signal_handler(signal.SIGINT, stop.set_result, None)
    await stop

    # 6. 优雅地关闭服务
    print("\n正在停止服务...")
    results_task.cancel()
    downloader.stop()
    crawler.stop()
    print("服务已停止。")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
