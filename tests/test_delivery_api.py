"""交付切片 API 行为测试：阻断项、幂等重试、集合不同 409、固化快照不可变、
组装后固化前被并发汇交改动则 409 且不发布混合切片。"""
import os
import sqlite3
import sys

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app import service  # noqa: E402
from app.db import read_tx, write_tx  # noqa: E402
from app.main import app  # noqa: E402


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "test.db"))
    with TestClient(app) as c:
        yield c


def rev(obs, rid, parents, species=None, count=None, delete=False):
    body = {"observation_id": obs, "revision_id": rid, "parents": parents,
            "delete": delete}
    if species is not None:
        body["species"] = species
    if count is not None:
        body["count"] = count
    return body


def pkg(*revs):
    return {"revisions": list(revs)}


def submit(client, *revs):
    r = client.post("/packages", json=pkg(*revs))
    assert r.status_code == 200, r.text
    return r


def deliver(client, delivery_id, observation_ids):
    return client.post(
        "/deliveries",
        json={"delivery_id": delivery_id, "observation_ids": observation_ids},
    )


def seed_valid(client, obs, rid="r1", species="A", count=1):
    submit(client, rev(obs, f"{obs}-{rid}", [], species=species, count=count))


# ------------------------------------------------------------ 正常发布

def test_deliver_valid_observations_freezes_values(client):
    seed_valid(client, "O1", species="Cervus elaphus", count=3)
    seed_valid(client, "O2", species="Lynx lynx", count=2)
    r = deliver(client, "D1", ["O1", "O2"])
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["delivery_id"] == "D1"
    assert body["observation_ids"] == ["O1", "O2"]
    assert body["existing"] is False
    items = body["items"]
    assert items[0] == {
        "observation_id": "O1",
        "revision_id": "O1-r1",
        "tips": ["O1-r1"],
        "species": "Cervus elaphus",
        "count": 3,
        "deleted": False,
        "version": 1,
    }
    assert items[1]["species"] == "Lynx lynx"

    got = client.get("/deliveries/D1")
    assert got.status_code == 200
    assert got.json() == body

    listed = client.get("/deliveries").json()
    assert [d["delivery_id"] for d in listed] == ["D1"]


def test_delivered_delete_marker_is_frozen(client):
    submit(client, rev("O1", "r1", [], species="A", count=1))
    submit(client, rev("O1", "r2", ["r1"], delete=True))
    r = deliver(client, "D1", ["O1"])
    assert r.status_code == 201
    assert r.json()["items"][0] == {
        "observation_id": "O1",
        "revision_id": "r2",
        "tips": ["r2"],
        "species": None,
        "count": None,
        "deleted": True,
        "version": 2,
    }


# ------------------------------------------------------------ 阻断项

def test_blockers_listed_and_no_slice_saved_then_retry_after_upload(client):
    seed_valid(client, "O1")
    submit(client, rev("O2", "O2-r2", ["O2-r1"], species="B", count=2))  # pending
    submit(client, rev("O3", "O3-a", [], species="C", count=1),
           rev("O3", "O3-b", [], species="C", count=2))                # conflict

    r = deliver(client, "D1", ["O1", "NOPE", "O2", "O3"])
    assert r.status_code == 422
    detail = r.json()["detail"]
    assert detail["message"]
    blockers = {b["observation_id"]: b for b in detail["blockers"]}
    assert blockers["NOPE"]["reason"] == "missing"
    assert blockers["O2"]["reason"] == "pending"
    assert blockers["O2"]["missing_ancestors"] == ["O2-r1"]
    assert blockers["O3"]["reason"] == "conflict"
    assert blockers["O3"]["tips"] == ["O3-a", "O3-b"]
    assert "O1" not in blockers

    # 不保存切片：查询 404，列表为空
    assert client.get("/deliveries/D1").status_code == 404
    assert client.get("/deliveries").json() == []

    # 补传：O2 的父到达，O3 人工裁决 → 同一交付号重试成功
    submit(client, rev("O2", "O2-r1", [], species="B", count=1))
    r = client.post("/observations/O3/resolutions",
                    json={"revision_id": "O3-fix", "parents": ["O3-a", "O3-b"],
                          "species": "C", "count": 1})
    assert r.status_code == 201
    r = deliver(client, "D1", ["O1", "O2", "O3"])
    assert r.status_code == 201
    assert [i["observation_id"] for i in r.json()["items"]] == ["O1", "O2", "O3"]


def test_blockers_include_all_items_not_just_first(client):
    r = deliver(client, "D1", ["X", "Y"])
    assert r.status_code == 422
    assert [b["observation_id"] for b in r.json()["detail"]["blockers"]] == ["X", "Y"]


# ------------------------------------------------------------ 幂等 / 409

def test_same_id_same_set_returns_original_slice(client):
    seed_valid(client, "O1")
    seed_valid(client, "O2")
    first = deliver(client, "D1", ["O1", "O2"])
    assert first.status_code == 201
    original = first.json()

    # 后续修订改变观测当前值
    submit(client, rev("O1", "O1-r2", ["O1-r1"], species="A", count=9))

    replay = deliver(client, "D1", ["O2", "O1"])  # 集合相同（顺序无关）即重试
    assert replay.status_code == 200
    body = replay.json()
    assert body["existing"] is True
    assert body["created_at"] == original["created_at"]
    assert body["items"] == original["items"]  # 已交付内容不随后续修订变化


def test_same_id_different_set_conflict(client):
    seed_valid(client, "O1")
    seed_valid(client, "O2")
    assert deliver(client, "D1", ["O1"]).status_code == 201
    r = deliver(client, "D1", ["O1", "O2"])
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["delivered_observation_ids"] == ["O1"]
    # 原切片未被覆盖
    assert client.get("/deliveries/D1").json()["observation_ids"] == ["O1"]


def test_unknown_delivery_404(client):
    assert client.get("/deliveries/NOPE").status_code == 404


# ------------------------------------------------------------ 入参校验

def test_duplicate_observation_ids_rejected(client):
    seed_valid(client, "O1")
    r = deliver(client, "D1", ["O1", "O1"])
    assert r.status_code == 422
    assert client.get("/deliveries/D1").status_code == 404


def test_at_most_100_observation_ids(client):
    submit(client, *[rev(f"O{i}", f"O{i}-r1", [], species="A", count=1)
                     for i in range(101)])
    r = deliver(client, "D-big", [f"O{i}" for i in range(101)])
    assert r.status_code == 422
    assert client.get("/deliveries/D-big").status_code == 404

    r = deliver(client, "D-100", [f"O{i}" for i in range(100)])
    assert r.status_code == 201
    assert len(r.json()["items"]) == 100


def test_empty_observation_ids_rejected(client):
    assert deliver(client, "D1", []).status_code == 422


# ------------------------------------------------------------ 固化前变化 → 409

def test_change_between_assembly_and_commit_aborts_without_mixed_slice(client):
    """组装读取后、固化前另一进程汇交改动了所选观测 → 409，不发布混合切片。"""
    seed_valid(client, "O1")
    seed_valid(client, "O2")

    # 1) 组装：在只读快照里得到候选切片
    with read_tx() as conn:
        candidate = service.assemble_delivery(conn, "D1", ["O1", "O2"])
    assert [i["revision_id"] for i in candidate["items"]] == ["O1-r1", "O2-r1"]

    # 2) 另一进程在组装之后、固化之前汇交
    with write_tx() as other:
        service.submit_package(
            other, [service.RevisionPayload(
                observation_id="O1", revision_id="O1-r2",
                parents=["O1-r1"], species="A", count=7, delete=False)]
        )

    # 3) 固化：指纹不符 → 409，整体回滚，不发布任何切片
    with pytest.raises(service.DeliveryConflict) as excinfo:
        with write_tx() as conn:
            service.commit_delivery(conn, candidate)
    assert "O1" in excinfo.value.extra["changed_observation_ids"]
    assert client.get("/deliveries/D1").status_code == 404
    assert client.get("/deliveries").json() == []
    # 观测本身仍是并发汇交后的新值，没有被旧组装结果覆盖
    assert client.get("/observations/O1").json()["effective"]["count"] == 7


def test_concurrent_commit_of_same_id_keeps_one_result(client):
    """并发提交同号：后到者返回先到者已固化的原切片，只留一份结果。"""
    seed_valid(client, "O1", species="A", count=4)

    with read_tx() as conn:
        candidate = service.assemble_delivery(conn, "D1", ["O1"])

    # 先到者在自己的写事务里固化
    with write_tx() as first:
        original = service.commit_delivery(first, candidate)
    # 后到者持有同一份候选（模拟并发）：看到已存在即原样返回
    with write_tx() as second:
        replay = service.commit_delivery(second, candidate)
    assert replay["existing"] is True
    assert replay["items"] == original["items"]
    assert replay["created_at"] == original["created_at"]

    assert len(client.get("/deliveries").json()) == 1
    again = deliver(client, "D1", ["O1"])
    assert again.status_code == 200
    assert again.json()["items"] == original["items"]


def test_assembly_reads_one_snapshot(client):
    """组装在一个只读快照内读取：期间另一进程的汇交不会混入本次组装。"""
    seed_valid(client, "O1", count=1)
    seed_valid(client, "O2", count=2)

    conn = sqlite3.connect(os.environ["DB_PATH"], timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("BEGIN")
        before = (service.get_observation(conn, "O1"),
                  service.get_observation(conn, "O2"))
        with write_tx() as other:
            service.submit_package(
                other, [service.RevisionPayload(
                    observation_id="O1", revision_id="O1-r2", parents=["O1-r1"],
                    species="A", count=5, delete=False)]
            )
        after = (service.get_observation(conn, "O1"),
                 service.get_observation(conn, "O2"))
        conn.execute("COMMIT")
    finally:
        conn.close()
    assert before == after  # 同一快照内多次读取看到同一已提交状态
    assert before[0]["version"] == 1


def test_assembly_after_blocker_resolved_by_other_process_uses_fresh_state(client):
    """组装时被阻断 → 另一进程补传 → 同号重试可成功（不缓存阻断结果）。"""
    submit(client, rev("O1", "O1-r2", ["O1-r1"], species="A", count=1))
    assert deliver(client, "D1", ["O1"]).status_code == 422
    submit(client, rev("O1", "O1-r1", [], species="A", count=1))
    r = deliver(client, "D1", ["O1"])
    assert r.status_code == 201
    assert r.json()["items"][0]["revision_id"] == "O1-r2"
