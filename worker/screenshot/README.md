# 截图核心库

本目录包含负责从 torrent 获取和处理视频数据的核心库。它被设计成一个独立的、高性能的模块，可以被其他应用程序使用（例如父目录中的 `worker.py`）。

## 核心组件

-   **`service.py` (`ScreenshotService`)**
    这是单个截图任务的主要协调器。它协调其他组件以执行从获取元数据到生成最终 JPG 文件的端到端工作流。它还处理错误分类（例如，可恢复 vs. 永久性失败）并生成用于容错的 `resume_data`。

-   **`client.py` (`TorrentClient`)**
    这是一个围绕 `libtorrent` 库的、复杂的、`asyncio` 友好的包装器。它处理所有底层的 BitTorrent 网络操作，例如管理会话、通过磁力链接添加 torrent、获取元数据以及按需下载特定数据块。它使用一个独立的线程来处理 `libtorrent` 的阻塞调用，以确保主 `asyncio` 事件循环永不阻塞。

-   **`extractor.py` (`KeyframeExtractor`)**
    一个健壮的 MP4 容器格式解析器。其主要工作是解析 `moov` atom（视频的“内容目录”），以构建一个包含所有视频帧（样本）的完整地图，而无需下载整个文件。它识别关键帧及其精确的字节级偏移量和大小，这对于最小化下载策略至关重要。它支持 H.264、H.265/HEVC 和 AV1 视频编解码器。

-   **`generator.py` (`ScreenshotGenerator`)**
    该组件负责最后阶段：解码原始视频帧数据并将其保存为 JPEG 图像。它使用 `PyAV` 库（它包装了 FFmpeg）并智能地将 CPU 密集型的解码工作卸载到独立的线程池中，以保持应用程序的响应性。它已被修改以支持实时的、每截图成功的回调。

-   **`config.py` (`Settings`)**
    使用 Pydantic-Settings 定义库的所有可配置参数（例如，超时、截图数量），允许通过环境变量或 `.env` 文件进行配置。

-   **`errors.py`**
    定义了一组自定义的、结构化的异常类（例如 `MetadataTimeoutError`、`MoovNotFoundError`），它们代表了截图过程中的特定失败模式。这允许调用应用程序进行清晰和精确的错误处理。
