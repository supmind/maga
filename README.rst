Maga DHT 爬虫和元数据获取工具 (单一文件版)
==================================================

.. contents:: 目录

引言
----

Maga 是一个基于 Python asyncio 开发的 DHT (分布式哈希表) 网络爬虫和 BitTorrent 元数据获取工具。
它的主要目的是在 DHT 网络中发现 BitTorrent 的 infohash，并连接到相关的 Peer 节点以下载和解析这些种子(torrent)的元数据（例如文件名、大小等信息）。
**当前版本已将原分散在 `maga.py`, `wire_protocol.py`, 和 `example.py` 中的核心代码统一整合到了单一文件 `dht_crawler.py` 中，方便部署和使用。文件内部的所有注释和文档字符串也已中文化。**

功能特性
--------

*   **DHT 节点发现**: 主动爬取 DHT 网络以查找和连接到其他节点，发现新的 infohash。
*   **元数据获取**: 实现 BEP 9 (BitTorrent 协议扩展之元数据交换)，能够连接到 Peer 节点并下载 torrent 元数据。
*   **异步核心**: 使用 `asyncio` 库进行高效的异步网络操作，支持大量并发连接。
*   **易于扩展**: 用户可以通过在 `dht_crawler.py` 中修改或继承 `Crawler` 类，并实现 `process_metadata_result` 方法来定义自己处理已获取元数据的逻辑。

安装
----

1.  获取代码:

    如果您通过克隆仓库方式获取，请定位到 `dht_crawler.py` 文件。

    .. code-block:: bash

        git clone https://github.com/yourusername/maga.git # 请替换为实际仓库URL
        cd maga
        # 主要代码现在位于 dht_crawler.py

2.  安装依赖:

    主要的第三方依赖是 `bencoder`。如果项目中提供了 `requirements.txt`，可以通过 pip 安装：

    .. code-block:: bash

        pip install -r requirements.txt
        # 或者单独安装: pip install bencoder

3.  Python 版本:

    建议使用 Python 3.7 或更高版本。

使用方法
--------

通过直接运行 `dht_crawler.py` 可以启动 DHT 爬虫实例。

.. code-block:: bash

    python dht_crawler.py

`dht_crawler.py` 文件中包含了 `Maga` 基类和 `Crawler` 示例实现。
`Crawler` 类展示了如何继承 `Maga` 基类并处理元数据获取结果。
核心在于实现 `process_metadata_result` 方法，该方法会在 `Maga` 成功（或失败）获取到一个 infohash 的元数据后被调用。

以下是 `dht_crawler.py` 中 `Crawler` 类的一个片段，展示了如何处理结果：

.. code-block:: python

    # dht_crawler.py (Crawler类中的示例代码)
    import logging
    import bencoder # 已在文件顶部导入

    # (假设 logging 已经通过 logging.basicConfig 配置好)

    class Crawler(Maga): # Maga 类也在 dht_crawler.py 中定义
        # ... (其他方法如 __init__, print_stats) ...

        async def process_metadata_result(self, metadata: bytes | None, infohash: str, peer_addr: tuple):
            # metadata: 获取到的元数据 (bytes类型)，获取失败则为 None
            # infohash: 相关的infohash (十六进制字符串)
            # peer_addr: 提供元数据的peer地址 (ip, port)

            self.crawler_logger.info(f"收到infohash结果: {infohash} 来自peer: {peer_addr}")

            if metadata:
                self.successful_fetches += 1
                self.crawler_logger.info(f"成功从 {peer_addr} 获取 {infohash} 的元数据。元数据大小: {len(metadata)} 字节。")
                try:
                    decoded_metadata = bencoder.bdecode(metadata)
                    if isinstance(decoded_metadata, dict):
                        name = decoded_metadata.get(b'name')
                        if name:
                            try:
                                self.crawler_logger.info(f"  名称: {name.decode('utf-8', errors='ignore')}")
                            except UnicodeDecodeError:
                                self.crawler_logger.info(f"  名称 (原始字节): {name}")
                        # ... (更多处理逻辑) ...
                except Exception as e:
                    self.crawler_logger.warning(f"  无法为 {infohash} 解码或处理元数据: {e}")
            else:
                self.failed_fetches += 1
                self.crawler_logger.warning(f"从 {peer_addr} 获取 {infohash} 的元数据失败。")

    # ... (文件末尾的 if __name__ == '__main__': 块) ...
    # if __name__ == '__main__':
    #     logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    #     crawler = Crawler(max_concurrent_metadata_fetches=5)
    #     if crawler.loop:
    #         crawler.loop.create_task(crawler.print_stats(interval=60))
    #     crawler.run(port=6881)


项目结构
--------

*   ``dht_crawler.py``: **核心文件**。包含了原 ``maga.py`` (DHT客户端逻辑), ``wire_protocol.py`` (元数据交换协议), 和 ``example.py`` (爬虫示例实现 `Crawler` 类) 的所有功能。此文件可以直接运行。
*   ``README.rst``: 本文档。
*   ``requirements.txt``: (如果提供) 项目所需的 Python 依赖库列表。

工作原理
--------

1.  **DHT 引导 (Bootstrap)**: 爬虫启动时 (运行 `dht_crawler.py`)，`Maga` 类逻辑会连接到一组预定义的引导节点（Bootstrap Nodes）。通过向这些节点发送 `find_node` 请求，逐步发现并填充自己的路由表，融入 DHT 网络。
2.  **请求处理**: `Maga` 类的逻辑处理 DHT 网络消息：
    *   当收到 `get_peers` 请求时，记录 infohash，并将请求来源节点视为潜在 peer。
    *   当收到 `announce_peer` 请求时，记录 infohash 和宣告的 peer 地址。
    *   以上两种情况都会触发内部的 `handler` 方法。
3.  **元数据获取**:
    *   `Maga` 类中的 `handler` 方法接收到 infohash 和 peer 地址后，会调用 `get_metadata` 函数 (此函数现在也位于 `dht_crawler.py` 中，内部使用 `WirePeerClient` 类)。
    *   `get_metadata` 函数尝试与目标 peer 建立 TCP 连接，进行 BitTorrent 协议握手和扩展协议握手 (ut_metadata)。
    *   如果对方支持，则分块请求元数据，下载完成后进行校验。
    *   `get_metadata` 返回完整的元数据内容或 `None`。
    *   最终，`Maga` 类中的 `handler` 会将 `get_metadata` 的结果传递给 `Crawler` 类中实现的 `process_metadata_result` 方法进行处理。

贡献
----

欢迎通过提交 问题 (Issues) 和 拉取请求 (Pull Requests) 来为项目做出贡献。

许可证
------

本项目使用 MIT 许可证。详情请参阅 ``LICENSE`` 文件 (如果项目中包含)。
