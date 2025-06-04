from maga import Maga

import logging
# 配置日志记录，设置级别为DEBUG，以捕获更详细的输出，包括Maga基类中的debug日志。
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Crawler类继承自Maga类。
# Maga类处理了DHT协议的复杂性，如节点发现、请求路由等。
# Crawler类的主要目的是通过覆盖Maga类中的特定方法（现在是process_metadata_result）
# 来定义当通过DHT网络发现infohash和潜在的peer地址，并且成功（或失败）获取元数据后，
# 应该执行的具体操作。
class Crawler(Maga):
    # process_metadata_result 是一个回调方法，当Maga基类通过get_metadata获取到
    # 一个infohash对应的元数据（或获取失败）时，此方法会被调用。
    # metadata: 获取到的元数据内容 (bytes类型，通常是bencoded字典)，如果获取失败则为None。
    # infohash: 相关的infohash (十六进制字符串格式)。
    # peer_addr: 提供元数据的peer的地址，格式为 (ip, port) 元组。
    async def process_metadata_result(self, metadata: bytes | None, infohash: str, peer_addr: tuple):
        # 首先记录接收到的infohash和peer地址，无论元数据获取是否成功。
        logging.info(f"Received result for infohash: {infohash} from peer: {peer_addr}")

        if metadata:
            # 如果metadata存在 (不是None)，说明元数据获取成功。
            # 注意：metadata是原始的bytes数据，如果它是bencoded字典，可能需要进一步解码才能查看其结构。
            # 此处仅作演示，直接记录原始数据（可能会很长，或包含非文本字符）。
            # 在实际应用中，你可能想要bdecode(metadata).get(b'name', b'N/A')等来获取文件名。
            logging.info(f"Successfully fetched metadata for {infohash} from {peer_addr}. Metadata size: {len(metadata)} bytes.")
            # 例如，尝试解码并打印文件名（如果存在）
            try:
                import bencoder
                decoded_metadata = bencoder.bdecode(metadata)
                if isinstance(decoded_metadata, dict):
                    name = decoded_metadata.get(b'name')
                    if name:
                        try:
                            logging.info(f"  Name: {name.decode('utf-8', errors='ignore')}")
                        except UnicodeDecodeError:
                            logging.info(f"  Name (raw bytes): {name}")
                    else:
                        logging.info(f"  Metadata does not contain a 'name' field.")
                else:
                    logging.info(f"  Metadata is not a dictionary.")
            except Exception as e:
                logging.warning(f"  Could not decode or process metadata for {infohash}: {e}")
        else:
            # 如果metadata为None，说明元数据获取失败。
            logging.warning(f"Failed to fetch metadata for {infohash} from {peer_addr}.")

# 创建Crawler实例
crawler = Crawler()
# 运行爬虫，监听DHT网络的默认端口6881。
# Maga基类的run方法会启动网络监听和DHT节点维护。
# 当通过get_peers或announce_peer发现infohash，并尝试从peer获取元数据后，
# 上面定义的process_metadata_result方法将被回调。
print("Example (stdout): Initializing Crawler and starting run...")
try:
    logging.getLogger().handlers[0].flush() # FLUSH LOGS
except:
    pass # Ignore
crawler.run(port=6881)
