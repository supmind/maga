# -*- coding: utf-8 -*-
"""
对 scheduler/crud.py 中数据库操作函数的单元测试。
"""
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from scheduler.database import Base
from scheduler import crud, models, schemas

# --- 测试数据库设置 ---
SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"
engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@pytest.fixture(scope="function")
def db_session():
    """
    一个 Pytest fixture，用于为每个测试函数提供一个独立的数据库会话。
    它会在测试开始前创建所有表，并在测试结束后删除所有表。
    """
    Base.metadata.create_all(bind=engine)
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()
        Base.metadata.drop_all(bind=engine)

# --- 测试用例 ---

def test_get_non_existent_task(db_session):
    """测试查询一个不存在的任务时应返回 None。"""
    task = crud.get_task_by_infohash(db_session, "non_existent_hash")
    assert task is None

def test_create_and_get_task(db_session):
    """测试任务的创建和根据 infohash 的查询功能。"""
    infohash = "test_infohash_123"
    task_create = schemas.TaskCreate(infohash=infohash)
    created_task = crud.create_task(db_session, task=task_create)
    assert created_task.infohash == infohash
    retrieved_task = crud.get_task_by_infohash(db_session, infohash=infohash)
    assert retrieved_task is not None
    assert retrieved_task.id == created_task.id

def test_get_non_existent_worker(db_session):
    """测试查询一个不存在的工作节点时应返回 None。"""
    worker = crud.get_worker_by_id(db_session, "non_existent_worker")
    assert worker is None

def test_create_and_get_worker(db_session):
    """测试工作节点的创建和根据 worker_id 的查询功能。"""
    worker_create = schemas.WorkerCreate(worker_id="worker-001", status="idle")
    created_worker = crud.create_worker(db_session, worker=worker_create)
    assert created_worker.worker_id == "worker-001"
    retrieved_worker = crud.get_worker_by_id(db_session, worker_id="worker-001")
    assert retrieved_worker is not None
    assert retrieved_worker.id == created_worker.id

def test_update_non_existent_worker_status(db_session):
    """测试更新一个不存在的工作节点状态时应返回 None。"""
    updated_worker = crud.update_worker_status(
        db_session, worker_id="non_existent_worker", status="busy",
        active_tasks_count=0, queue_size=0
    )
    assert updated_worker is None

def test_update_worker_status(db_session):
    """测试更新工作节点状态的功能。"""
    crud.create_worker(db_session, worker=schemas.WorkerCreate(worker_id="worker-002", status="idle"))
    updated_worker = crud.update_worker_status(
        db_session, worker_id="worker-002", status="busy",
        active_tasks_count=1, queue_size=1
    )
    assert updated_worker.status == "busy"
    assert updated_worker.active_tasks_count == 1
    assert updated_worker.queue_size == 1

def test_get_and_assign_next_task(db_session):
    """测试原子性地获取并分配下一个待处理任务的功能。"""
    crud.create_task(db_session, task=schemas.TaskCreate(infohash="task_to_be_assigned"))
    task = crud.get_and_assign_next_task(db_session, worker_id="worker-003")
    assert task is not None
    assert task.status == "working"
    next_task = crud.get_and_assign_next_task(db_session, worker_id="worker-004")
    assert next_task is None

def test_update_non_existent_task_status(db_session):
    """测试更新一个不存在的任务状态时应返回 None。"""
    updated_task = crud.update_task_status(db_session, infohash="non_existent_hash", status="success")
    assert updated_task is None

def test_update_task_status(db_session):
    """测试更新任务最终状态的功能。"""
    crud.create_task(db_session, task=schemas.TaskCreate(infohash="task_to_update"))
    crud.get_and_assign_next_task(db_session, worker_id="worker-005")

    message = "Completed with data"
    updated_task = crud.update_task_status(db_session, infohash="task_to_update", status="success", message=message)

    assert updated_task.status == "success"
    assert updated_task.assigned_worker_id is None
    assert updated_task.result_message == message

def test_record_screenshot_for_non_existent_task(db_session):
    """测试为不存在的任务记录截图时应返回 None。"""
    result = crud.record_screenshot(db_session, "non_existent_hash", "file.jpg")
    assert result is None

def test_record_screenshot_multiple_times(db_session):
    """
    测试对同一个任务多次调用 record_screenshot，验证所有记录都被正确添加。
    """
    infohash = "multiple_records_task"
    crud.create_task(db_session, task=schemas.TaskCreate(infohash=infohash))

    crud.record_screenshot(db_session, infohash, "file1.jpg")
    crud.record_screenshot(db_session, infohash, "file2.jpg")
    crud.record_screenshot(db_session, infohash, "file1.jpg") # 测试重复添加
    crud.record_screenshot(db_session, infohash, "file3.jpg")

    final_task = crud.get_task_by_infohash(db_session, infohash)
    assert final_task is not None
    final_screenshots = final_task.successful_screenshots

    assert isinstance(final_screenshots, list)
    assert len(final_screenshots) == 3
    assert set(final_screenshots) == {"file1.jpg", "file2.jpg", "file3.jpg"}


def test_upsert_worker_creation(db_session):
    """测试当工作节点不存在时，upsert_worker 是否能正确创建新的工作节点。"""
    worker_id = "new_worker_for_upsert"
    worker = crud.upsert_worker(db_session, worker_id=worker_id, status="idle")
    assert worker is not None
    assert worker.worker_id == worker_id
    assert worker.status == "idle"
    retrieved_worker = crud.get_worker_by_id(db_session, worker_id)
    assert retrieved_worker is not None
    assert retrieved_worker.id == worker.id


def test_upsert_worker_update(db_session):
    """测试当工作节点已存在时，upsert_worker 是否能正确更新其信息。"""
    worker_id = "existing_worker_for_upsert"
    # 首先创建一个工作节点
    crud.create_worker(db_session, schemas.WorkerCreate(worker_id=worker_id, status="idle"))

    # 然后使用 upsert 更新它
    updated_worker = crud.upsert_worker(db_session, worker_id=worker_id, status="busy")

    assert updated_worker is not None
    assert updated_worker.status == "busy"
    assert updated_worker.last_seen_at is not None

    retrieved_worker = crud.get_worker_by_id(db_session, worker_id)
    assert retrieved_worker.status == "busy"


def test_reset_stuck_tasks(db_session):
    """测试 reset_stuck_tasks 是否能正确地将卡死的任务状态重置为 'pending'。"""
    from datetime import datetime, timedelta

    # 1. 创建一个任务并分配给一个 worker，使其状态变为 'working'
    infohash = "stuck_task_hash"
    worker_id = "worker_for_stuck_task"
    crud.create_task(db_session, schemas.TaskCreate(infohash=infohash))
    task = crud.get_and_assign_next_task(db_session, worker_id=worker_id)
    assert task.status == 'working'

    # 2. 手动将任务的 updated_at 时间戳更新为过去的时间，模拟卡死状态
    stuck_time = datetime.utcnow() - timedelta(hours=1)
    db_session.query(models.Task).filter_by(infohash=infohash).update({"updated_at": stuck_time}, synchronize_session=False)
    db_session.commit()

    # 3. 执行重置操作
    timeout_seconds = 30 * 60  # 30 分钟
    reset_count = crud.reset_stuck_tasks(db_session, timeout_seconds)

    # 4. 验证结果
    assert reset_count == 1

    updated_task = crud.get_task_by_infohash(db_session, infohash)
    assert updated_task.status == 'pending'
    assert updated_task.assigned_worker_id is None
