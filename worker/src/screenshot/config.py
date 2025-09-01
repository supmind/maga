# -*- coding: utf-8 -*-
"""
此模块使用 Pydantic-Settings 定义了服务的全局配置。

它允许从环境变量或 .env 文件中加载配置，从而将配置与代码分离。
"""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    定义了服务的所有可配置参数。

    Pydantic-Settings 会自动尝试从匹配的环境变量（不区分大小写）中读取这些值。
    例如，`num_workers` 的值可以被环境变量 `NUM_WORKERS` 覆盖。
    """
    # 如果存在 .env 文件，则从中加载环境变量
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    # --- 工作进程配置 ---
    num_workers: int = 10

    # --- 路径配置 ---
    output_dir: str = './screenshots_output'
    torrent_save_path: str | None = None

    # --- 截图选择策略配置 ---
    min_screenshots: int = 5
    max_screenshots: int = 50
    target_interval_sec: int = 180
    default_screenshots: int = 20

    # --- 超时配置 ---
    metadata_timeout: int = 180
    moov_probe_timeout: int = 120
    piece_fetch_timeout: int = 60
    piece_queue_timeout: int = 300
