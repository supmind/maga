Maga DHT 爬虫和元数据获取工具
====================================

.. contents:: 目录

引言
----

Maga 是一个基于 Python asyncio 开发的 DHT (分布式哈希表) 网络爬虫和 BitTorrent 元数据获取工具。
它的主要目的是在 DHT 网络中发现 BitTorrent 的 infohash，并连接到相关的 Peer 节点以下载和解析这些种子(torrent)的元数据（例如文件名、大小等信息）。

功能特性
--------

*   **DHT 节点发现**: 主动爬取 DHT 网络以查找和连接到其他节点，发现新的 infohash。
*   **元数据获取**: 实现 BEP 9 (BitTorrent 协议扩展之元数据交换)，能够连接到 Peer 节点并下载 torrent 元数据。
*   **异步核心**: 使用 `asyncio` 库进行高效的异步网络操作，支持大量并发连接。
*   **易于扩展**: 用户可以通过继承并实现特定方法（如 `process_metadata_result`）来定义自己处理已获取元数据的逻辑。

安装
----

1.  克隆仓库:

    .. code-block:: bash

        git clone https://github.com/yourusername/maga.git
        cd maga

    *(请将 ``https://github.com/yourusername/maga.git`` 替换为实际的仓库URL)*

2.  安装依赖:

    .. code-block:: bash

        pip install -r requirements.txt

3.  Python 版本:

    建议使用 Python 3.7 或更高版本。

使用方法
--------

通过运行 `example.py` 可以快速启动一个基本的 DHT 爬虫实例。

.. code-block:: bash

    python example.py

`example.py` 中的 `Crawler` 类展示了如何继承 `Maga` 基类并处理元数据获取结果。
核心在于实现 `process_metadata_result` 方法，该方法会在 `Maga` 成功（或失败）获取到一个 infohash 的元数据后被调用。

以下是 `example.py` 中 `Crawler` 类的一个示例片段，展示了如何处理结果：

.. code-block:: python

    # example.py (示例代码)
    import logging
    from maga import Maga
    # 假设元数据是bencoded编码的，可能需要bdecode来解析
    # from bencoder import bdecode # 如果在 process_metadata_result 中实际解码，请取消注释

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    class Crawler(Maga):
        async def process_metadata_result(self, metadata: bytes | None, infohash: str, peer_addr: tuple):
            # metadata: 获取到的元数据 (bytes类型)，获取失败则为 None
            # infohash: 相关的infohash (十六进制字符串)
            # peer_addr: 提供元数据的peer地址 (ip, port)

            logging.info(f"从节点 {peer_addr} 收到关于 {infohash} 的元数据处理请求")

            if metadata:
                try:
                    # 尝试解码元数据 (假设它是bencoded)
                    # 注意：实际的 bdecode 函数需要从相应库导入，例如 'bencoder'
                    # from bencoder import bdecode (应在文件顶部导入)
                    # decoded_metadata = bdecode(metadata)
                    # torrent_name = decoded_metadata.get(b'name', b'N/A').decode('utf-8', 'ignore')
                    # logging.info(f"成功获取 {infohash} 的元数据 (来自 {peer_addr}): 名称 - {torrent_name}")

                    # 此处仅记录元数据大小作为示例
                    logging.info(f"成功获取 {infohash} 的元数据 (来自 {peer_addr})。大小: {len(metadata)} 字节。")

                    # 在此可以进一步处理元数据, 例如:
                    # 1. 使用 bdecode 解析 metadata
                    # 2. 提取文件名、文件列表、大小等信息
                    # 3. 将信息保存到数据库或文件
                    # 4. 甚至可以基于元数据内容决定是否下载完整 torrent (需额外实现)

                except Exception as e:
                    logging.error(f"处理元数据 {infohash} (来自 {peer_addr}) 时出错: {e}")
            else:
                logging.warning(f"未能从 {peer_addr} 获取 {infohash} 的元数据。")

    if __name__ == '__main__':
        crawler = Crawler()
        # 运行DHT爬虫，默认监听6881端口
        # Maga类的构造函数接受 bootstrap_nodes, interval, loop 等参数用于高级配置
        crawler.run(port=6881)

项目结构
--------

*   ``maga.py``: 核心 DHT 客户端逻辑。处理 DHT 节点间的通信、路由表维护、infohash 和 peer 的发现。
*   ``wire_protocol.py``: BitTorrent 线路协议中用于元数据交换部分（BEP 9）的实现。包含 ``WirePeerClient`` 类和 ``get_metadata`` 函数，负责与 peer 建立连接并获取元数据。
*   ``example.py``: 展示如何使用 ``Maga`` 类进行 DHT 爬取和元数据处理的示例代码。
*   ``README.rst``: 本文档，提供项目介绍、安装和使用说明。
*   ``requirements.txt``: 项目所需的 Python 依赖库列表。

工作原理
--------

1.  **DHT 引导 (Bootstrap)**: 爬虫启动时，会连接到一组预定义的引导节点（Bootstrap Nodes）。通过向这些节点发送 `find_node` 请求，逐步发现并填充自己的路由表，融入 DHT 网络。
2.  **请求处理**:
    *   当收到 `get_peers` 请求时，Maga 会记录下请求中的 infohash，并认为请求来源节点是一个潜在的 peer。然后调用内部的 `handler` 方法。
    *   当收到 `announce_peer` 请求时，Maga 会记录 infohash 和宣告的 peer 地址（IP和端口）。然后调用内部的 `handler` 方法。
3.  **元数据获取**:
    *   内部 `handler` 方法接收到 infohash 和 peer 地址后，会调用 `wire_protocol.py` 中的 `get_metadata(infohash_bytes, peer_ip, peer_port, loop)` 函数。
    *   `get_metadata` 函数（内部使用 `WirePeerClient`）会尝试与目标 peer 建立 TCP 连接。
    *   连接成功后，进行 BitTorrent 协议握手，并发送扩展协议握手，表明希望进行元数据交换 (ut_metadata)。
    *   如果对方支持，则分块请求元数据。每一块元数据下载完成后，会进行校验。
    *   所有元数据块下载完成并校验通过后，`get_metadata` 返回完整的元数据内容。如果失败，则返回 `None`。
    *   最终，`Maga` 类中的 `handler` 会将 `get_metadata` 的结果传递给用户在子类中实现的 `process_metadata_result` 方法进行处理。

贡献
----

欢迎通过提交 问题 (Issues) 和 拉取请求 (Pull Requests) 来为项目做出贡献。

许可证
------

本项目使用 MIT 许可证。详情请参阅 ``LICENSE`` 文件。
