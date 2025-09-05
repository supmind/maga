# -*- coding: utf-8 -*-
"""
对 scheduler/main.py 中 FastAPI 端点的单元测试。
"""
import pytest
import os
import json
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from io import BytesIO

from scheduler.main import app, get_db
from scheduler.database import Base
from scheduler import crud, schemas
from config import Settings

# --- 测试数据库设置 ---
SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"
engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# --- Fixtures ---

@pytest.fixture(scope="function")
def client():
    """
    一个 Pytest fixture，为每个测试函数提供一个配置好测试数据库的 TestClient。
    """
    def override_get_db():
        db = TestingSessionLocal()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db
    Base.metadata.create_all(bind=engine)
    yield TestClient(app)
    Base.metadata.drop_all(bind=engine)
    app.dependency_overrides.clear()

@pytest.fixture(scope="session")
def api_key_headers():
    """提供带有正确 API 密钥的请求头。"""
    settings = Settings()
    return {"X-API-Key": settings.scheduler_api_key}

# --- 测试用例 ---

def test_read_root(client):
    """测试根端点，它不应该需要认证。"""
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "截图调度器正在运行"}

# --- 认证测试 ---

def test_endpoint_no_api_key(client):
    """测试在没有提供 API 密钥时，受保护的端点是否会拒绝访问。"""
    response = client.post("/tasks/", data={"infohash": "some_hash"})
    assert response.status_code == 403
    assert "无效的 API Key 或未提供" in response.json()["detail"]

def test_endpoint_wrong_api_key(client):
    """测试在提供了错误的 API 密钥时，受保护的端点是否会拒绝访问。"""
    response = client.post(
        "/tasks/",
        data={"infohash": "some_hash"},
        headers={"X-API-Key": "wrong_key"}
    )
    assert response.status_code == 403
    assert "无效的 API Key 或未提供" in response.json()["detail"]

# --- 任务管理端点测试 (带认证) ---

def test_create_task_new(client, api_key_headers):
    infohash = "new_task_hash"
    response = client.post("/tasks/", data={"infohash": infohash}, headers=api_key_headers)
    assert response.status_code == 201
    data = response.json()
    assert data["infohash"] == infohash
    assert data["status"] == "pending"

def test_create_task_with_torrent_file(client, api_key_headers):
    infohash = "task_with_file"
    torrent_content = b"d8:announce4:test4:infod6:lengthi1e4:name4:testee"
    response = client.post(
        "/tasks/",
        data={"infohash": infohash},
        files={"torrent_file": ("test.torrent", BytesIO(torrent_content), "application/x-bittorrent")},
        headers=api_key_headers
    )
    assert response.status_code == 201

    metadata_dir = "temp_metadata"
    metadata_file = os.path.join(metadata_dir, f"{infohash}.torrent")
    assert os.path.exists(metadata_file)

    # 清理
    os.remove(metadata_file)
    if not os.listdir(metadata_dir):
        os.rmdir(metadata_dir)

def test_create_task_existing_pending(client, api_key_headers):
    infohash = "existing_pending_hash"
    client.post("/tasks/", data={"infohash": infohash}, headers=api_key_headers)
    response = client.post("/tasks/", data={"infohash": infohash}, headers=api_key_headers)
    assert response.status_code == 200
    assert response.json()["infohash"] == infohash

def test_create_task_permanent_failure(client, api_key_headers):
    infohash = "permanent_failure_hash"
    db = TestingSessionLocal()
    crud.create_task(db, schemas.TaskCreate(infohash=infohash))
    crud.update_task_status(db, infohash, "permanent_failure")
    db.close()

    response = client.post("/tasks/", data={"infohash": infohash}, headers=api_key_headers)
    assert response.status_code == 400
    assert "永久失败" in response.text

def test_create_task_recoverable_failure(client, api_key_headers):
    infohash = "recoverable_failure_hash"
    db = TestingSessionLocal()
    crud.create_task(db, schemas.TaskCreate(infohash=infohash))
    crud.update_task_status(db, infohash, "recoverable_failure")
    db.close()

    response = client.post("/tasks/", data={"infohash": infohash}, headers=api_key_headers)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "pending"

def test_get_task_by_infohash(client, api_key_headers):
    infohash = "get_by_hash"
    client.post("/tasks/", data={"infohash": infohash}, headers=api_key_headers)
    response = client.get(f"/tasks/{infohash}", headers=api_key_headers)
    assert response.status_code == 200
    assert response.json()["infohash"] == infohash

def test_get_nonexistent_task(client, api_key_headers):
    response = client.get("/tasks/nonexistent_hash", headers=api_key_headers)
    assert response.status_code == 404

def test_get_next_task(client, api_key_headers):
    infohash = "next_task_hash"
    client.post("/tasks/", data={"infohash": infohash}, headers=api_key_headers)
    response = client.get("/tasks/next", params={"worker_id": "worker-1"}, headers=api_key_headers)
    assert response.status_code == 200
    assert response.json()["infohash"] == infohash

def test_get_next_task_no_tasks(client, api_key_headers):
    response = client.get("/tasks/next", params={"worker_id": "worker-1"}, headers=api_key_headers)
    assert response.status_code == 200
    assert response.json() is None

def test_get_next_task_with_metadata_file(client, api_key_headers):
    infohash = "next_with_metadata"
    metadata_dir = "temp_metadata"
    os.makedirs(metadata_dir, exist_ok=True)
    metadata_file = os.path.join(metadata_dir, f"{infohash}.torrent")
    with open(metadata_file, "wb") as f:
        f.write(b"test content")

    client.post("/tasks/", data={"infohash": infohash}, headers=api_key_headers)
    response = client.get("/tasks/next", params={"worker_id": "worker-1"}, headers=api_key_headers)
    assert response.status_code == 200
    assert os.path.exists(metadata_file)

    os.remove(metadata_file)
    if not os.listdir(metadata_dir):
        os.rmdir(metadata_dir)

def test_worker_registration_and_reregistration(client, api_key_headers):
    worker_id = "test_worker_001"
    response1 = client.post("/workers/register", json={"worker_id": worker_id, "status": "idle"}, headers=api_key_headers)
    assert response1.status_code == 200
    first_seen_at = response1.json()["last_seen_at"]
    response2 = client.post("/workers/register", json={"worker_id": worker_id, "status": "idle"}, headers=api_key_headers)
    assert response2.status_code == 200
    assert response2.json()["last_seen_at"] >= first_seen_at

def test_worker_heartbeat(client, api_key_headers):
    worker_id = "test_worker_002"
    client.post("/workers/register", json={"worker_id": worker_id, "status": "idle"}, headers=api_key_headers)
    heartbeat_payload = {"worker_id": worker_id, "status": "busy", "active_tasks_count": 1, "queue_size": 5}
    response = client.post("/workers/heartbeat", json=heartbeat_payload, headers=api_key_headers)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "busy"

def test_record_screenshot_endpoint(client, api_key_headers):
    infohash = "record_screenshot_hash"
    client.post("/tasks/", data={"infohash": infohash}, headers=api_key_headers)
    payload = {"filename": "screenshot_01.jpg"}
    response = client.post(f"/tasks/{infohash}/screenshots", json=payload, headers=api_key_headers)
    assert response.status_code == 200
    assert "screenshot_01.jpg" in response.json()["successful_screenshots"]

def test_update_status_success_deletes_metadata(client, api_key_headers):
    infohash = "success_deletes_metadata"
    metadata_dir = "temp_metadata"
    os.makedirs(metadata_dir, exist_ok=True)
    metadata_file = os.path.join(metadata_dir, f"{infohash}.torrent")
    with open(metadata_file, "wb") as f:
        f.write(b"test content")
    client.post("/tasks/", data={"infohash": infohash}, headers=api_key_headers)
    payload = {"status": "success", "message": "All done!"}
    response = client.post(f"/tasks/{infohash}/status", json=payload, headers=api_key_headers)
    assert response.status_code == 200
    assert not os.path.exists(metadata_file)
    if not os.listdir(metadata_dir):
        os.rmdir(metadata_dir)

def test_update_status_failure_preserves_metadata(client, api_key_headers):
    infohash = "failure_preserves_metadata"
    metadata_dir = "temp_metadata"
    os.makedirs(metadata_dir, exist_ok=True)
    metadata_file = os.path.join(metadata_dir, f"{infohash}.torrent")
    with open(metadata_file, "wb") as f:
        f.write(b"test content")
    client.post("/tasks/", data={"infohash": infohash}, headers=api_key_headers)
    payload = {"status": "recoverable_failure", "message": "An error"}
    response = client.post(f"/tasks/{infohash}/status", json=payload, headers=api_key_headers)
    assert response.status_code == 200
    assert os.path.exists(metadata_file)
    os.remove(metadata_file)
    os.rmdir(metadata_dir)

def test_update_status_with_resume_data_creates_file(client, api_key_headers):
    infohash = "status_with_resume"
    client.post("/tasks/", data={"infohash": infohash}, headers=api_key_headers)
    resume_data = {"key": "value"}
    payload = {"status": "recoverable_failure", "message": "Error with resume data", "resume_data": resume_data}
    resume_dir = "resume_data"
    resume_file = os.path.join(resume_dir, f"{infohash}.resume")
    if os.path.exists(resume_file): os.remove(resume_file)
    if os.path.exists(resume_dir) and not os.listdir(resume_dir): os.rmdir(resume_dir)
    response = client.post(f"/tasks/{infohash}/status", json=payload, headers=api_key_headers)
    assert response.status_code == 200
    assert os.path.exists(resume_file)
    with open(resume_file, "r") as f:
        saved_data = json.load(f)
    assert saved_data == resume_data
    os.remove(resume_file)
    os.rmdir(resume_dir)

def test_get_next_task_with_resume_file(client, api_key_headers):
    infohash = "next_with_resume_file"
    client.post("/tasks/", data={"infohash": infohash}, headers=api_key_headers)
    resume_dir = "resume_data"
    os.makedirs(resume_dir, exist_ok=True)
    resume_file = os.path.join(resume_dir, f"{infohash}.resume")
    resume_data = {"state": "halfway"}
    with open(resume_file, "w") as f:
        json.dump(resume_data, f)
    response = client.get("/tasks/next", params={"worker_id": "worker-1"}, headers=api_key_headers)
    assert response.status_code == 200
    assert response.json()["resume_data"] == resume_data
    assert not os.path.exists(resume_file)
    if not os.listdir(resume_dir):
        os.rmdir(resume_dir)

def test_list_all_tasks_with_filtering_and_pagination(client, api_key_headers):
    db = TestingSessionLocal()
    for i in range(5):
        crud.create_task(db, schemas.TaskCreate(infohash=f"pending_{i:02d}"))
    for i in range(3):
        task = crud.create_task(db, schemas.TaskCreate(infohash=f"success_{i:02d}"))
        crud.update_task_status(db, task.infohash, "success")
    db.close()
    response_all = client.get("/tasks/all/", headers=api_key_headers)
    assert response_all.json()["total"] == 8
    response_pending = client.get("/tasks/all/", params={"status": "pending"}, headers=api_key_headers)
    assert response_pending.json()["total"] == 5
    response_success = client.get("/tasks/all/", params={"status": "success"}, headers=api_key_headers)
    assert response_success.json()["total"] == 3

def test_update_task_details_endpoint(client, api_key_headers):
    infohash = "details_update_hash"
    client.post("/tasks/", data={"infohash": infohash}, headers=api_key_headers)
    details_payload = {"torrent_name": "My Test Torrent", "video_filename": "movie.mp4", "video_duration_seconds": 3600}
    response = client.post(f"/tasks/{infohash}/details", json=details_payload, headers=api_key_headers)
    assert response.status_code == 200
    assert response.json()["torrent_name"] == "My Test Torrent"
