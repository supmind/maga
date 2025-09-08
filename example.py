import asyncio
import signal
import binascii
import os
import collections
from datetime import datetime

import aiohttp
from fastbencode import bencode
from elasticsearch_async import AsyncElasticsearch
from maga.crawler import Maga
from maga.downloader import get_metadata

# API端点，用于添加新任务
API_URL = "http://47.79.229.105:8000/tasks/"

# Elasticsearch 配置
ES_HOST = "localhost"
ES_PORT = 9200
ES_INDEX_NAME = "torrents"

# Peer Checker 配置
PEER_CHECKER_INTERVAL_SECONDS = 300  # 任务运行间隔
HIGH_PRIORITY_PEER_THRESHOLD = 50   # 高优层节点数阈值
HIGH_PRIORITY_TIER_HOURS_AGO = 12   # 高优层检查时间范围
TRENDING_TIER_HOURS_AGO = 6         # 趋势层检查时间范围
HIGH_PRIORITY_BATCH_SIZE = 50       # 高优层每批次大小
TRENDING_BATCH_SIZE = 100           # 趋势层每批次大小
SWEEP_BATCH_SIZE = 100              # 扫描层每批次大小


class BoundedSet:
    """
    一个有固定大小的集合，用于存储最近处理过的infohash。
    当集合满时，会自动丢弃最旧的元素。
    这能有效防止内存无限增长。
    """
    def __init__(self, max_size=1_000_000):
        self.max_size = max_size
        self.deque = collections.deque()
        self.set = set()

    def add(self, item):
        # 假设在调用add之前，已经检查过item是否存在
        if len(self.deque) == self.max_size:
            oldest = self.deque.popleft()
            self.set.remove(oldest)

        self.deque.append(item)
        self.set.add(item)

    def __contains__(self, item):
        return item in self.set

# 使用一个有界集合来记录已经处理过的infohash，防止内存无限增长
PROCESSED_INFOHASHES = BoundedSet(max_size=1_000_000)

# 确保保存.torrent文件的目录存在
os.makedirs("torrents", exist_ok=True)


def format_bytes(size):
    """将字节大小格式化为可读的字符串（KB, MB, GB等）"""
    if size is None:
        return "N/A"
    power = 1024
    n = 0
    power_labels = {0: '', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    while size > power and n < len(power_labels) - 1:
        size /= power
        n += 1
    return f"{size:.2f} {power_labels[n]}B"


def contains_mp4(info):
    """
    检查种子元数据(info字典)中是否包含.mp4文件。
    支持单文件和多文件种子。
    """
    # 检查多文件种子
    if b'files' in info and info[b'files']:
        for file_info in info[b'files']:
            # file_info[b'path'] 是一个路径段列表，最后一个是文件名
            if file_info[b'path']:
                try:
                    # 获取文件名并解码，忽略解码错误，转为小写进行不区分大小写的比较
                    filename = file_info[b'path'][-1].decode(errors='ignore').lower()
                    if filename.endswith('.mp4'):
                        return True
                except Exception:
                    # 如果解码或其他操作失败，则跳过此文件
                    continue
    # 检查单文件种子
    elif b'name' in info:
        try:
            # 获取文件名并解码，忽略解码错误，转为小写
            filename = info[b'name'].decode(errors='ignore').lower()
            if filename.endswith('.mp4'):
                return True
        except Exception:
            # 如果解码失败，则认为不包含
            return False

    # 如果遍历完所有文件都未找到.mp4，则返回False
    return False


async def add_task_to_downloader(session, infohash_hex, torrent_file_path):
    """
    异步函数，用于将infohash和种子文件上传到下载器以添加新任务
    """
    try:
        # 构建 multipart/form-data 请求体
        data = aiohttp.FormData()
        data.add_field('infohash', infohash_hex)

        # 打开并添加种子文件到请求体
        # aiohttp 会自动处理文件的异步读取
        with open(torrent_file_path, 'rb') as torrent_file:
            data.add_field('torrent_file',
                           torrent_file,
                           filename=os.path.basename(torrent_file_path),
                           content_type='application/x-bittorrent')

            # 发送POST请求，复用传入的session
            async with session.post(API_URL, data=data) as response:
                if response.status == 200 or response.status == 201:
                    print(f"  [API] 成功上传并添加任务: {infohash_hex}")
                else:
                    # 打印带有状态码和响应内容的错误信息
                    response_text = await response.text()
                    print(f"  [API] 添加任务失败: {infohash_hex}, "
                          f"状态码: {response.status}, 响应: {response_text}")

    except aiohttp.ClientError as e:
        # 捕获网络连接相关的错误
        print(f"  [API] 请求失败: {infohash_hex} -> {e}")
    except FileNotFoundError:
        print(f"  [API] 找不到要上传的种子文件: {torrent_file_path}")
    except Exception as e:
        # 捕获其他未知异常
        print(f"  [API] 发生未知错误: {infohash_hex} -> {e}")


async def create_es_index_if_not_exists(es_client):
    """
    如果索引不存在，则创建它，并设置好支持中文检索的mapping。
    """
    # 检查索引是否存在
    if not await es_client.indices.exists(index=ES_INDEX_NAME):
        # 定义索引的mapping
        mapping = {
            "mappings": {
                "properties": {
                    "infohash": {"type": "keyword"},
                    "name": {"type": "text", "analyzer": "cjk"},
                    "files": {
                        "type": "nested",
                        "properties": {
                            "path": {"type": "text", "analyzer": "cjk"},
                            "length": {"type": "long"}
                        }
                    },
                    "total_size": {"type": "long"},
                    "created_at": {"type": "date"},
                    "peer_count": {"type": "integer"},
                    "discovery_count_since_last_check": {"type": "integer"},
                    "last_checked": {"type": "date"}
                }
            }
        }
        try:
            # 创建索引
            await es_client.indices.create(index=ES_INDEX_NAME, body=mapping)
            print(f"[ES] Index '{ES_INDEX_NAME}' created successfully.")
        except Exception as e:
            print(f"[ES] Error creating index '{ES_INDEX_NAME}': {e}")
            # 如果创建失败，可能意味着并发冲突，再检查一次
            if not await es_client.indices.exists(index=ES_INDEX_NAME):
                raise e # 如果还是不存在，则抛出异常


async def save_metadata_to_es(es_client, infohash_hex, info):
    """
    将获取到的元数据格式化并保存到Elasticsearch中。
    """
    try:
        now = datetime.utcnow()
        # 准备要索引的文档
        doc = {
            'infohash': infohash_hex,
            'name': info.get(b'name', b'Unknown').decode(errors='ignore'),
            'created_at': now,
            'peer_count': 0,
            'discovery_count_since_last_check': 1,
            'last_checked': now
        }

        # 处理文件列表和总大小
        if b'files' in info and info[b'files']:
            doc['files'] = [
                {'path': '/'.join([p.decode(errors='ignore') for p in f.get(b'path', [])]), 'length': f.get(b'length', 0)}
                for f in info[b'files']
            ]
            doc['total_size'] = sum(f['length'] for f in doc['files'])
        else:
            doc['files'] = []
            doc['total_size'] = info.get(b'length', 0)

        # 使用infohash作为文档ID，可以避免重复索引
        await es_client.index(index=ES_INDEX_NAME, id=infohash_hex, body=doc)
        print(f"  [ES] 成功索引新种子: {infohash_hex}")

    except Exception as e:
        print(f"  [ES] 索引新种子失败: {infohash_hex} -> {e}")


from datetime import timedelta

async def peer_checker_task(crawler, es_client):
    """
    一个后台任务，定期检查种子的节点数并更新其热度。
    """
    while True:
        try:
            # 每N秒检查一次
            await asyncio.sleep(PEER_CHECKER_INTERVAL_SECONDS)
            print("[Peer Checker] 开始检查节点数...")

            now = datetime.utcnow()

            # 1. 高优层查询：获取已知热门，且在N小时内未检查过的种子
            high_priority_query = {
                "sort": [{"last_checked": "asc"}],
                "query": {
                    "bool": {
                        "must": [
                            {"range": {"peer_count": {"gt": HIGH_PRIORITY_PEER_THRESHOLD}}}
                        ],
                        "filter": {
                            "range": {
                                "last_checked": {
                                    "lt": (now - timedelta(hours=HIGH_PRIORITY_TIER_HOURS_AGO)).isoformat()
                                }
                            }
                        }
                    }
                }
            }
            res = await es_client.search(index=ES_INDEX_NAME, body=high_priority_query, size=HIGH_PRIORITY_BATCH_SIZE)
            torrents_to_check = res['hits']['hits']
            if torrents_to_check:
                print(f"[Peer Checker] 高优层发现 {len(torrents_to_check)} 个种子需要检查。")
            else:
                # 2. 趋势层查询：获取最近发现次数最多，且在N小时内未检查过的种子
                trending_query = {
                    "sort": [{"discovery_count_since_last_check": "desc"}],
                    "query": {
                        "bool": {
                            "filter": {
                                "range": {
                                    "last_checked": {
                                        "lt": (now - timedelta(hours=TRENDING_TIER_HOURS_AGO)).isoformat()
                                    }
                                }
                            }
                        }
                    }
                }
                res = await es_client.search(index=ES_INDEX_NAME, body=trending_query, size=TRENDING_BATCH_SIZE)
                torrents_to_check = res['hits']['hits']
                print(f"[Peer Checker] 趋势层发现 {len(torrents_to_check)} 个种子需要检查。")

            # 3. 如果趋势层和高优层都没有，则启动扫描层
            if not torrents_to_check:
                query = {
                    "sort": [{"last_checked": "asc"}],
                    "query": {
                        "match_all": {}
                    }
                }
                res = await es_client.search(index=ES_INDEX_NAME, body=query, size=SWEEP_BATCH_SIZE)
                torrents_to_check = res['hits']['hits']
                print(f"[Peer Checker] 扫描层发现 {len(torrents_to_check)} 个种子需要检查。")

            for torrent in torrents_to_check:
                infohash_hex = torrent['_id']
                infohash_bytes = binascii.unhexlify(infohash_hex)

                print(f"[Peer Checker] 正在检查: {infohash_hex}")
                peer_count = await crawler.get_peers_recursive(infohash_bytes)

                # 更新文档
                update_body = {
                    "doc": {
                        "peer_count": peer_count,
                        "last_checked": datetime.utcnow(),
                        "discovery_count_since_last_check": 0 # 重置发现计数
                    }
                }
                await es_client.update(index=ES_INDEX_NAME, id=infohash_hex, body=update_body)
                print(f"[Peer Checker] 更新完毕: {infohash_hex}, 发现 {peer_count} 个节点。")

        except asyncio.CancelledError:
            print("[Peer Checker] 任务被取消，正在退出...")
            break
        except Exception as e:
            print(f"[Peer Checker] 发生错误: {e}")


async def discovery_updater_task(queue, es_client):
    """
    消费者任务，从队列中获取infohash并批量更新到Elasticsearch。
    """
    while True:
        try:
            batch = []
            # 从队列中获取第一个项目，会在此等待直到有项目可用
            first_item = await queue.get()
            batch.append(first_item)

            # 尝试获取更多项目以进行批处理，但不阻塞
            while len(batch) < 200 and not queue.empty():
                batch.append(queue.get_nowait())

            # 准备批量更新请求
            bulk_operations = []
            for infohash_hex in batch:
                op = {
                    "update": {
                        "_index": ES_INDEX_NAME,
                        "_id": infohash_hex
                    }
                }
                script = {
                    "script": {
                        "source": "ctx._source.discovery_count_since_last_check += 1",
                        "lang": "painless"
                    }
                }
                bulk_operations.append(op)
                bulk_operations.append(script)

            if bulk_operations:
                await es_client.bulk(body=bulk_operations)
                # print(f"[Discovery Updater] 批量更新了 {len(batch)} 个种子的发现次数。")

        except asyncio.CancelledError:
            print("[Discovery Updater] 任务被取消，正在退出...")
            break
        except Exception as e:
            print(f"[Discovery Updater] 发生错误: {e}")


async def main():
    loop = asyncio.get_running_loop()
    # 创建一个信号量，限制并发下载任务为100
    download_semaphore = asyncio.Semaphore(100)
    # 创建一个队列，用于解耦发现和数据库更新
    discovery_queue = asyncio.Queue(maxsize=10000)

    # 创建一个可复用的 aiohttp.ClientSession 和 AsyncElasticsearch 客户端
    # 使用 async with 来确保在程序退出时资源被正确关闭
    async with aiohttp.ClientSession() as session, \
               AsyncElasticsearch(hosts=[{'host': ES_HOST, 'port': ES_PORT}]) as es_client:

        # 准备Elasticsearch
        await create_es_index_if_not_exists(es_client)

        # 定义当爬虫发现新infohash时的回调函数
        # 这个函数可以访问外部作用域的 session, es_client, semaphore 和 queue 变量
        async def on_infohash_discovered(infohash, peer_addr):
            infohash_hex = binascii.hexlify(infohash).decode()

            # 生产者：将发现的infohash放入队列，这是一个非阻塞操作
            try:
                discovery_queue.put_nowait(infohash_hex)
            except asyncio.QueueFull:
                pass  # 如果队列满了，暂时丢弃，避免阻塞

            # 使用信号量来控制并发，仅对新种子进行元数据下载
            async with download_semaphore:
                # 如果这个infohash还没有被处理过
                if infohash_hex not in PROCESSED_INFOHASHES:
                    PROCESSED_INFOHASHES.add(infohash_hex)

                    # 直接调用下载器函数，传入infohash和peer地址
                    # get_metadata返回的直接就是info字典
                    info = await get_metadata(infohash, peer_addr[0], peer_addr[1], loop=loop)

                    # 只有在成功获取到元数据(info字典)时才打印信息
                    if info:
                        # 为了生成一个有效的.torrent文件，我们需要一个顶层的字典
                        torrent_dict = {b'info': info}

                        # 提取核心信息用于打印
                        name = info.get(b'name', b'Unknown').decode(errors='ignore')
                        if b'files' in info:
                            num_files = len(info[b'files'])
                            total_size = sum(f[b'length'] for f in info[b'files'])
                        else:
                            num_files = 1
                            total_size = info.get(b'length')

                        # 将元数据保存到.torrent文件
                        file_path = os.path.join("torrents", f"{infohash_hex}.torrent")
                        try:
                            with open(file_path, "wb") as f:
                                # 我们需要对包含info字典的顶层字典进行bencode编码
                                f.write(bencode(torrent_dict))

                            # 打印摘要
                            print("=" * 30 + " 下载成功 " + "=" * 30)
                            print(f"  Infohash: {infohash_hex}")
                            print(f"  文件名: {name}")
                            print(f"  文件数: {num_files}")
                            print(f"  总大小: {format_bytes(total_size)}")
                            print(f"  已保存到: {file_path}")

                            # ======================================================
                            # 检查是否包含 .mp4 文件, 如果包含则提交任务
                            # ======================================================
                            if contains_mp4(info):
                                print(f"  [检查] 元数据中发现 .mp4 文件, 准备提交任务。")
                                # 传入infohash、文件路径和共享的session
                                await add_task_to_downloader(session, infohash_hex, file_path)
                            else:
                                print(f"  [检查] 元数据中未发现 .mp4 文件, 跳过任务提交。")

                            print("=" * 70 + "\n")

                        except Exception as e:
                            # 仅在保存失败时打印错误
                            print(f"[保存失败] {infohash_hex} -> {e}")

                        # 无论其他操作是否成功，都尝试将元数据保存到Elasticsearch
                        await save_metadata_to_es(es_client, infohash_hex, info)

        # 创建并运行爬虫
        crawler = Maga(loop=loop, handler=on_infohash_discovered)
        await crawler.run(port=6981)

        # 启动后台任务
        checker_task = asyncio.create_task(peer_checker_task(crawler, es_client))
        updater_task = asyncio.create_task(discovery_updater_task(discovery_queue, es_client))
        print("后台任务（节点检查、发现次数更新）已启动。")

        print("服务已启动，正在后台监听和下载...")
        print("只有成功下载的种子才会被打印出来。")
        print("按 Ctrl+C 停止运行。")

        # 等待程序被中断
        stop = asyncio.Future()
        loop.add_signal_handler(signal.SIGINT, stop.set_result, None)
        await stop

        # 优雅地关闭服务
        print("\n正在停止服务...")
        checker_task.cancel()
        updater_task.cancel()
        crawler.stop()
        await asyncio.gather(checker_task, updater_task, return_exceptions=True)
        print("服务已停止。")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
