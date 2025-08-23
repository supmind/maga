import asyncio
import signal
import binascii

from maga.crawler import Maga
from maga.downloader import Downloader

# 使用一个集合（set）来记录已经提交给下载器的infohash
# 这是为了防止爬虫因为频繁收到同一个infohash的announce消息而重复提交任务
SUBMITTED_INFOHASHES = set()


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
            print(f"[爬虫] 发现新种源: {infohash_hex}，已提交给下载器。")
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

    print("\n爬虫和下载器服务已开始运行。")
    print("爬虫正在监听新的种子...")
    print("下载器正在等待任务并下载元数据...")
    print("按 Ctrl+C 停止运行。")

    # 5. 等待程序被中断 (Ctrl+C)
    stop = asyncio.Future()
    loop.add_signal_handler(signal.SIGINT, stop.set_result, None)
    await stop

    # 6. 优雅地关闭服务
    print("\n正在停止服务...")
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
