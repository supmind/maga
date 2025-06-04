from maga import Maga

import logging
# 配置日志记录，设置级别为DEBUG，以捕获更详细的输出，包括Maga基类中的debug日志。
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

import asyncio

# Crawler类继承自Maga类。
# Maga类处理了DHT协议的复杂性，如节点发现、请求路由等。
# Crawler类的主要目的是通过覆盖Maga类中的特定方法（现在是process_metadata_result）
# 来定义当通过DHT网络发现infohash和潜在的peer地址，并且成功（或失败）获取元数据后，
# 应该执行的具体操作。
class Crawler(Maga):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(__name__) # Use the module's logger, or a specific one for Crawler
        self.successful_fetches = 0
        self.failed_fetches = 0
        self.last_successful_fetches = 0
        self.last_failed_fetches = 0

    # process_metadata_result 是一个回调方法，当Maga基类通过get_metadata获取到
    # 一个infohash对应的元数据（或获取失败）时，此方法会被调用。
    # metadata: 获取到的元数据内容 (bytes类型，通常是bencoded字典)，如果获取失败则为None。
    # infohash: 相关的infohash (十六进制字符串格式)。
    # peer_addr: 提供元数据的peer的地址，格式为 (ip, port) 元组。
    async def process_metadata_result(self, metadata: bytes | None, infohash: str, peer_addr: tuple):
        # 首先记录接收到的infohash和peer地址，无论元数据获取是否成功。
        # Note: The base Maga class already logs attempt and success/failure if process_metadata_result is overridden.
        # We can keep these logs or rely on base class logging + stats. For now, keeping them.
        self.logger.info(f"Received result for infohash: {infohash} from peer: {peer_addr}")

        if metadata:
            self.successful_fetches += 1
            # 如果metadata存在 (不是None)，说明元数据获取成功。
            self.logger.info(f"Successfully fetched metadata for {infohash} from {peer_addr}. Metadata size: {len(metadata)} bytes.")
            # 例如，尝试解码并打印文件名（如果存在）
            try:
                import bencoder
                decoded_metadata = bencoder.bdecode(metadata)
                if isinstance(decoded_metadata, dict):
                    name = decoded_metadata.get(b'name')
                    if name:
                        try:
                            self.logger.info(f"  Name: {name.decode('utf-8', errors='ignore')}")
                        except UnicodeDecodeError:
                            self.logger.info(f"  Name (raw bytes): {name}")
                    else:
                        self.logger.info(f"  Metadata does not contain a 'name' field.")
                else:
                    self.logger.info(f"  Metadata is not a dictionary.")
            except Exception as e:
                self.logger.warning(f"  Could not decode or process metadata for {infohash}: {e}")
        else:
            self.failed_fetches += 1
            # 如果metadata为None，说明元数据获取失败。
            self.logger.warning(f"Failed to fetch metadata for {infohash} from {peer_addr}.")

    async def print_stats(self, interval=60):
        self.logger.info(f"Performance stats printing started. Reporting interval: {interval}s")
        while True:
            await asyncio.sleep(interval)
            current_success = self.successful_fetches
            current_failed = self.failed_fetches

            delta_success = current_success - self.last_successful_fetches
            delta_failed = current_failed - self.last_failed_fetches

            self.logger.info(
                f"Stats: Success (last {interval}s): {delta_success}, "
                f"Failed (last {interval}s): {delta_failed}. "
                f"Total Success: {current_success}, Total Failed: {current_failed}"
            )

            self.last_successful_fetches = current_success
            self.last_failed_fetches = current_failed

# 创建Crawler实例
# Demonstrate the new parameter by setting max_concurrent_metadata_fetches to 5
crawler = Crawler(max_concurrent_metadata_fetches=5)

# Schedule the print_stats task
# Accessing crawler.loop which is initialized in Maga's __init__
crawler.loop.create_task(crawler.print_stats(interval=60))


# 运行爬虫，监听DHT网络的默认端口6881。
# Maga基类的run方法会启动网络监听和DHT节点维护。
# 当通过get_peers或announce_peer发现infohash，并尝试从peer获取元数据后，
# 上面定义的process_metadata_result方法将被回调。
print("Example (stdout): Initializing Crawler and starting run...") # This print is fine for console feedback
try:
    logging.getLogger().handlers[0].flush() # FLUSH LOGS
except:
    pass # Ignore
crawler.run(port=6881)
