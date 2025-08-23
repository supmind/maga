import asyncio
import signal
import binascii
import logging
import os
import pprint

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

            # 按用户要求，只在成功后打印解码的元数据信息
            print("="*30 + f" METADATA FOR {infohash_hex} " + "="*29)
            pprint.pprint(metadata)
            print("="*80)
            print(f"[保存成功] {file_path} 已保存。\n")

        except Exception:
            # 隐藏保存时的错误信息，保持安静
            pass
        finally:
            # 通知队列这项任务已完成
            downloader.results_queue.task_done()


# async def print_dht_stats(downloader):
#     """
#     一个辅助函数，每10秒打印一次DHT路由表的状态
#     """
#     while True:
#         await asyncio.sleep(10)
#         # 从downloader服务中获取其内部的dht_node实例，并打印路由表节点总数
#         table_size = len(downloader.dht_node.routing_table.get_all_nodes())
#         print(f"[状态] DHT路由表当前包含 {table_size} 个节点。")


async def main():
    # 获取当前的asyncio事件循环
    loop = asyncio.get_running_loop()

    # 1. 创建下载器（Downloader）服务实例
    # 这个服务会运行自己的DHT节点（用于查找peers）和一个下载工人池
    downloader = Downloader(loop=loop, dht_port=6882, num_workers=10)

    # 2. 为爬虫（Crawler）定义回调函数（handler）
    # 当爬虫发现一个新的infohash时，这个函数就会被调用
    # 它的任务是把infohash提交给下载器的任务队列
    async def crawler_handler(infohash, peer_addr):
        # 将bytes类型的infohash转换为十六进制字符串，方便记录和打印
        infohash_hex = binascii.hexlify(infohash).decode()

        # 如果这个infohash还没有被处理过
        if infohash_hex not in SUBMITTED_INFOHASHES:
            # 将它添加到已处理集合中，避免重复
            SUBMITTED_INFOHASHES.add(infohash_hex)
            # print(f"[爬虫] 发现新种源: {infohash_hex}，已提交给下载器。") # 注释掉以减少输出
            # 异步地将infohash（bytes类型）提交到下载器的队列
            await downloader.submit(infohash)

    # 3. 创建爬虫（Crawler）服务实例
    # 我们把上面定义的回调函数传递给它
    crawler = Maga(loop=loop, handler=crawler_handler)

    # 4. 启动两个服务
    # 下载器服务运行在6882端口
    await downloader.run()

    # 爬虫服务运行在6881端口
    await crawler.run(port=6881)

    # 启动后台任务
    # stats_task = loop.create_task(print_dht_stats(downloader)) # 注释掉以减少输出
    results_task = loop.create_task(process_results(downloader))

    print("服务已启动，正在后台爬取和下载...")
    print("成功下载的元数据内容将被打印出来。")
    print("按 Ctrl+C 停止运行。")

    # 5. 等待程序被中断 (Ctrl+C)
    stop = asyncio.Future()
    loop.add_signal_handler(signal.SIGINT, stop.set_result, None)
    await stop

    # 6. 优雅地关闭服务
    print("\n正在停止服务...")
    # stats_task.cancel() # 注释掉以减少输出
    results_task.cancel()
    downloader.stop()
    crawler.stop()
    print("服务已停止。")


if __name__ == "__main__":
    try:
        # 运行主协程
        asyncio.run(main())
    except KeyboardInterrupt:
        # 捕获Ctrl+C，防止打印多余的出错信息
        pass
