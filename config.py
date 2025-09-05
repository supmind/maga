# -*- coding: utf-8 -*-
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """
    存储所有应用配置的类。
    配置从 .env 文件加载。
    """
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # --- 调度器设置 ---
    scheduler_url: str = "http://127.0.0.1:8000"
    db_url: str = "sqlite:///./scheduler.db"
    scheduler_api_key: str = "a_very_secret_and_complex_key_for_dev" # 用于保护调度器 API 的密钥

    # --- 工作节点设置 ---
    num_workers: int = 50  # 单个 ScreenshotService 实例中用于处理任务的内部工作协程数量
    worker_idle_time: int = 30  # seconds
    worker_max_queue_size: int = 20  # 工作节点在暂停从调度器获取新任务前，其内部任务队列的最大长度

    # --- 截图服务设置 ---
    output_dir: str = "./screenshots_output"  # 存储最终截图的目录
    torrent_save_path: str = "./torrent_files"  # 存储 .torrent 文件的目录
    min_screenshots: int = 5
    max_screenshots: int = 60
    default_screenshots: int = 20
    target_interval_sec: int = 180

    # --- Libtorrent 性能调优设置 ---
    # 这些设置直接影响 libtorrent 会话的性能，对于高负载环境至关重要。
    # 请根据您的硬件资源（CPU, 内存, 网络）和预期的负载来调整它们。
    lt_listen_interfaces: str = "0.0.0.0:6881"  # libtorrent 监听的端口和网络接口
    lt_active_limit: int = 500  # 同时处理的活动 torrent 最大数量
    lt_active_downloads: int = 500  # 同时处理的下载中 torrent 最大数量
    lt_connections_limit: int = 4096  # 全局最大连接数
    lt_cache_size: int = 2048  # 磁盘 I/O 缓存大小，单位为 16KB 块 (例如 2048 * 16KB = 32MB)
    lt_upload_rate_limit: int = 0  # 全局上传速率限制，单位 B/s (0 表示无限制)
    lt_download_rate_limit: int = 0  # 全局下载速率限制，单位 B/s (0 表示无限制)
    lt_peer_connect_timeout: int = 10  # 连接 peer 的超时时间（秒）

    # Timeouts for various operations
    metadata_timeout: int = 180  # 3 minutes
    moov_probe_timeout: int = 120  # 2 minutes
    piece_fetch_timeout: int = 60  # 1 minute
    piece_queue_timeout: int = 300  # 5 minutes

    # --- R2 存储设置 ---
    # 请在 .env 文件中配置这些值
    r2_endpoint_url: str = ""
    r2_access_key_id: str = ""
    r2_secret_access_key: str = ""
    r2_bucket_name: str = ""
