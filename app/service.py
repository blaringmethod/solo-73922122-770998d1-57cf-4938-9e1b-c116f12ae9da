"""核心域逻辑：修订 DAG 的接收、校验与观测状态重算。

每个观测的修订构成一张有向无环图（允许子先父后乱序到达）：
- pending  待补齐：已知修订引用了尚未到达的祖先，不出有效值；补齐后自动重算；
- valid    有效：祖先齐全且只剩一个无后继修订（当前分支唯一），其载荷即有效值
           （删除标记也是合法结论，表示该观测被删除）；
- conflict 冲突：祖先齐全但存在多个无后继修订（多分支；改值与删除并存同属此类），
           不能输出单一有效观测，需人工裁决。

人工裁决：以裁决时所见全部分支（tips）为父新建修订；提交时分支已变则 409；
裁决之后迟到的并发分支会重新形成冲突，不会被裁决吞没。

交付切片：按交付号从同一已提交状态读取一批观测的当前分支与有效值；任一观测
不存在/待补齐/冲突则列出阻断项、不保存切片；全部可用才在单个写事务内一次性
固化（观测号、修订号、物种、数量、删除标记），全部 SELECT 之后再写，保证
其他进程的汇交不会让切片混入不同时点的数据；所选观测在固化前变化则 409。
同号重试返回原切片；观测号集合不同则 409；并发提交同号只留一份结果。
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Optional


class PackageRejected(Exception):
    """整包拒绝：事务回滚，不留任何部分记录。"""


class ObservationNotFound(Exception):
    pass


class BranchesChanged(Exception):
    """裁决提交时分支已变化（409）。"""

    def __init__(self, observation_id: str, current_tips: list[str], status: str):
        super().__init__(observation_id)
        self.observation_id = observation_id
        self.current_tips = current_tips
        self.status = status


class DeliveryBlocked(Exception):
    """交付切片存在阻断项：不保存切片，修正后可重试。"""

    def __init__(self, blockers: list[dict]):
        super().__init__("交付切片存在阻断项")
        self.blockers = blockers


class DeliveryConflict(Exception):
    """交付号已存在但观测号集合不同，或固化前所选观测发生变化（409）。"""

    def __init__(self, message: str, **extra):
        super().__init__(message)
        self.message = message
        self.extra = extra


@dataclass
class RevisionPayload:
    observation_id: str
    revision_id: str
    parents: list[str]
    species: Optional[str]
    count: Optional[int]
    delete: bool


def canonical_content(rev: RevisionPayload) -> str:
    """规范化内容指纹：同号同内容重传幂等的比对基准（父修订顺序无关）。"""
    return json.dumps(
        {
            "observation_id": rev.observation_id,
            "parents": sorted(set(rev.parents)),
            "species": rev.species,
            "count": rev.count,
            "delete": bool(rev.delete),
        },
        ensure_ascii=False,
        sort_keys=True,
    )


# ---------------------------------------------------------------- 查询

def _row_to_state(row: sqlite3.Row) -> dict:
    effective = None
    if row["status"] == "valid" and row["eff_revision_id"] is not None:
        effective = {
            "revision_id": row["eff_revision_id"],
            "species": row["eff_species"],
            "count": row["eff_count"],
            "deleted": bool(row["eff_deleted"]),
        }
    return {
        "observation_id": row["observation_id"],
        "status": row["status"],
        "tips": json.loads(row["tips"]),
        "missing_ancestors": json.loads(row["missing"]),
        "effective": effective,
        "version": row["version"],
    }


def get_observation(conn: sqlite3.Connection, observation_id: str) -> Optional[dict]:
    row = conn.execute(
        "SELECT * FROM observations WHERE observation_id = ?", (observation_id,)
    ).fetchone()
    return _row_to_state(row) if row else None


def list_observations(conn: sqlite3.Connection, status: Optional[str] = None) -> list[dict]:
    if status:
        rows = conn.execute(
            "SELECT * FROM observations WHERE status = ? ORDER BY observation_id", (status,)
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM observations ORDER BY observation_id").fetchall()
    return [_row_to_state(r) for r in rows]


def list_revisions(conn: sqlite3.Connection, observation_id: str) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM revisions WHERE observation_id = ? ORDER BY received_at, revision_id",
        (observation_id,),
    ).fetchall()
    parents = conn.execute(
        "SELECT rp.revision_id, rp.parent_id FROM revision_parents rp "
        "JOIN revisions rv ON rv.revision_id = rp.revision_id "
        "WHERE rv.observation_id = ?",
        (observation_id,),
    ).fetchall()
    pmap: dict[str, list[str]] = {}
    for p in parents:
        pmap.setdefault(p["revision_id"], []).append(p["parent_id"])
    return [
        {
            "revision_id": r["revision_id"],
            "observation_id": r["observation_id"],
            "parents": sorted(pmap.get(r["revision_id"], [])),
            "species": r["species"],
            "count": r["individual_count"],
            "delete": bool(r["is_delete"]),
            "received_at": r["received_at"],
        }
        for r in rows
    ]


# ---------------------------------------------------------------- 重算

def recompute_observation(conn: sqlite3.Connection, observation_id: str) -> dict:
    """按当前已到的全部修订重算观测状态（待补齐/有效/冲突）。"""
    rows = conn.execute(
        "SELECT revision_id, species, individual_count, is_delete "
        "FROM revisions WHERE observation_id = ?",
        (observation_id,),
    ).fetchall()
    ids = {r["revision_id"] for r in rows}

    # 当前分支 = 没有任何后继（子修订）的修订
    referenced: set[str] = set()
    if ids:
        marks = ",".join("?" for _ in ids)
        referenced = {
            r[0]
            for r in conn.execute(
                f"SELECT DISTINCT parent_id FROM revision_parents WHERE parent_id IN ({marks})",
                sorted(ids),
            )
        }
    tips = sorted(ids - referenced)

    # 未齐祖先 = 本观测修订引用、但尚未到达的修订号
    needed: set[str] = set()
    if ids:
        needed = {
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT rp.parent_id FROM revision_parents rp "
                "JOIN revisions rv ON rv.revision_id = rp.revision_id "
                "WHERE rv.observation_id = ?",
                (observation_id,),
            )
        }
    missing = sorted(needed - ids)

    if missing:
        status = "pending"
        eff = (None, None, None, 0)
    elif len(tips) == 1:
        tip = next(r for r in rows if r["revision_id"] == tips[0])
        status = "valid"
        eff = (tip["revision_id"], tip["species"], tip["individual_count"], tip["is_delete"])
    else:
        status = "conflict"
        eff = (None, None, None, 0)

    tips_json = json.dumps(tips, ensure_ascii=False)
    missing_json = json.dumps(missing, ensure_ascii=False)
    prev = conn.execute(
        "SELECT * FROM observations WHERE observation_id = ?", (observation_id,)
    ).fetchone()
    if prev is None:
        conn.execute(
            "INSERT INTO observations (observation_id, status, tips, missing, "
            "eff_revision_id, eff_species, eff_count, eff_deleted, version) "
            "VALUES (?,?,?,?,?,?,?,?,1)",
            (observation_id, status, tips_json, missing_json, *eff),
        )
    else:
        current = (
            prev["status"], prev["tips"], prev["missing"], prev["eff_revision_id"],
            prev["eff_species"], prev["eff_count"], prev["eff_deleted"],
        )
        if current != (status, tips_json, missing_json, *eff):
            conn.execute(
                "UPDATE observations SET status=?, tips=?, missing=?, eff_revision_id=?, "
                "eff_species=?, eff_count=?, eff_deleted=?, version=version+1 "
                "WHERE observation_id=?",
                (status, tips_json, missing_json, *eff, observation_id),
            )
    return get_observation(conn, observation_id)  # type: ignore[return-value]


# ---------------------------------------------------------------- 汇交

def submit_package(conn: sqlite3.Connection, revisions: list[RevisionPayload]) -> dict:
    """接收一个 JSON 包：整体校验、整体入库、整体重算；任何一条非法则整包拒绝。"""
    # 1) 包内去重：同号同内容幂等，同号异内容整包拒绝
    unique: dict[str, tuple[str, RevisionPayload]] = {}
    for rev in revisions:
        content = canonical_content(rev)
        prev = unique.get(rev.revision_id)
        if prev is not None and prev[0] != content:
            raise PackageRejected(f"修订 {rev.revision_id} 在包内同号异内容")
        unique.setdefault(rev.revision_id, (content, rev))

    # 2) 与库中已有修订比对：同号同内容→幂等跳过；同号异内容→整包拒绝
    existing: dict[str, sqlite3.Row] = {}
    if unique:
        marks = ",".join("?" for _ in unique)
        existing = {
            r["revision_id"]: r
            for r in conn.execute(
                f"SELECT revision_id, observation_id, content FROM revisions "
                f"WHERE revision_id IN ({marks})",
                list(unique),
            )
        }
    duplicates: list[str] = []
    fresh: dict[str, tuple[str, RevisionPayload]] = {}
    for rid, (content, rev) in unique.items():
        hit = existing.get(rid)
        if hit is None:
            fresh[rid] = (content, rev)
        elif hit["content"] != content:
            raise PackageRejected(f"修订 {rid} 与库中已有修订同号异内容")
        else:
            duplicates.append(rid)

    states: list[dict] = []
    if fresh:
        _validate_new_revisions(conn, fresh, existing)
        # 3) 全部校验通过后整体入库
        for rid, (content, rev) in fresh.items():
            conn.execute(
                "INSERT INTO revisions (revision_id, observation_id, species, "
                "individual_count, is_delete, content) VALUES (?,?,?,?,?,?)",
                (rid, rev.observation_id, rev.species, rev.count,
                 1 if rev.delete else 0, content),
            )
            for parent in sorted(set(rev.parents)):
                conn.execute(
                    "INSERT INTO revision_parents (revision_id, parent_id) VALUES (?,?)",
                    (rid, parent),
                )
        # 4) 受影响观测自动重算（补齐的祖先会解除待补齐，新分支会形成冲突等）
        for obs_id in sorted({rev.observation_id for _, rev in fresh.values()}):
            states.append(recompute_observation(conn, obs_id))

    return {
        "accepted": sorted(fresh),
        "duplicates": sorted(duplicates),
        "observations": states,
    }


def _validate_new_revisions(
    conn: sqlite3.Connection,
    fresh: dict[str, tuple[str, RevisionPayload]],
    existing: dict[str, sqlite3.Row],
) -> None:
    # 取出新修订引用、且已在库中的父修订（不在包内的那些）
    ref_parents: set[str] = set()
    for _, rev in fresh.values():
        ref_parents.update(rev.parents)
    ref_parents -= set(fresh)
    ref_parents -= set(existing)
    parent_rows: dict[str, sqlite3.Row] = {}
    if ref_parents:
        marks = ",".join("?" for _ in ref_parents)
        parent_rows = {
            r["revision_id"]: r
            for r in conn.execute(
                f"SELECT revision_id, observation_id FROM revisions "
                f"WHERE revision_id IN ({marks})",
                sorted(ref_parents),
            )
        }

    # 正向跨观测引用：父修订（无论在包内还是库中）必须属于同一观测
    for rid, (_, rev) in fresh.items():
        for parent in set(rev.parents):
            if parent == rid:
                raise PackageRejected(f"修订 {rid} 以自身为父，构成循环")
            if parent in fresh:
                if fresh[parent][1].observation_id != rev.observation_id:
                    raise PackageRejected(f"修订 {rid} 跨观测引用父修订 {parent}")
            elif parent in existing:
                if existing[parent]["observation_id"] != rev.observation_id:
                    raise PackageRejected(f"修订 {rid} 跨观测引用父修订 {parent}")
            elif parent in parent_rows:
                if parent_rows[parent]["observation_id"] != rev.observation_id:
                    raise PackageRejected(f"修订 {rid} 跨观测引用父修订 {parent}")
            # 父未到达：允许（子先父后），观测标待补齐

    # 循环检测：库中 DAG 本无环，只需检查包内新修订之间的边
    indegree = {rid: 0 for rid in fresh}
    children: dict[str, list[str]] = {rid: [] for rid in fresh}
    for rid, (_, rev) in fresh.items():
        for parent in set(rev.parents):
            if parent in fresh:
                indegree[rid] += 1
                children[parent].append(rid)
    stack = [rid for rid, deg in indegree.items() if deg == 0]
    visited = 0
    while stack:
        node = stack.pop()
        visited += 1
        for child in children[node]:
            indegree[child] -= 1
            if indegree[child] == 0:
                stack.append(child)
    if visited != len(fresh):
        raise PackageRejected("修订之间存在循环引用")

    # 反向跨观测引用：新修订是库中既有子修订（先到的子）的父
    for rid, (_, rev) in fresh.items():
        rows = conn.execute(
            "SELECT rv.revision_id, rv.observation_id FROM revision_parents rp "
            "JOIN revisions rv ON rv.revision_id = rp.revision_id WHERE rp.parent_id = ?",
            (rid,),
        ).fetchall()
        for row in rows:
            if row["observation_id"] != rev.observation_id:
                raise PackageRejected(
                    f"修订 {rid} 与既有子修订 {row['revision_id']} 跨观测引用"
                )


# ---------------------------------------------------------------- 交付切片

MAX_DELIVERY_ITEMS = 100


def _fingerprint(conn: sqlite3.Connection, observation_id: str) -> Optional[tuple]:
    """观测当前状态的指纹：用于判断组装后、固化前是否被并发汇交改动。

    状态版本在每次状态变化时 +1；修订总数与状态版本合在一起，可识别任何
    新修订入库（修订不可变，有效值只可能随修订集合变化）。
    """
    row = conn.execute(
        "SELECT version, status, tips FROM observations WHERE observation_id = ?",
        (observation_id,),
    ).fetchone()
    if row is None:
        return None
    count = conn.execute(
        "SELECT COUNT(*) FROM revisions WHERE observation_id = ?", (observation_id,)
    ).fetchone()[0]
    return (row["version"], row["status"], row["tips"], count)


def _blocker_from_state(state: Optional[dict], observation_id: str) -> Optional[dict]:
    """按观测状态给出阻断项；可用（valid）时返回 None。"""
    if state is None:
        return {
            "observation_id": observation_id,
            "reason": "missing",
            "status": "missing",
            "message": "观测不存在",
        }
    if state["status"] == "valid":
        return None
    return {
        "observation_id": observation_id,
        "reason": state["status"],  # pending | conflict
        "status": state["status"],
        "message": "观测待补齐" if state["status"] == "pending" else "观测存在冲突",
        "tips": state["tips"],
        "missing_ancestors": state["missing_ancestors"],
    }


def _blocker(conn: sqlite3.Connection, observation_id: str) -> Optional[dict]:
    return _blocker_from_state(get_observation(conn, observation_id), observation_id)


def assemble_delivery(
    conn: sqlite3.Connection, delivery_id: str, observation_ids: list[str]
) -> dict:
    """组装阶段（只读快照）：从同一已提交状态读取全部所选观测。

    - 同号且观测号集合一致：直接返回原切片（不重新组装）；
    - 同号但集合不同：409；
    - 任一观测不存在/待补齐/冲突：列出全部阻断项，不保存切片；
    - 全部可用：返回候选切片与各观测指纹，供固化阶段校验是否被并发改动。
    """
    existing = replay_existing_delivery(conn, delivery_id, observation_ids)
    if existing is not None:
        return existing

    states: list[dict] = []
    blockers: list[dict] = []
    fingerprints: dict[str, tuple] = {}
    for obs_id in observation_ids:
        state = get_observation(conn, obs_id)
        blocker = _blocker_from_state(state, obs_id)
        if blocker is not None:
            blockers.append(blocker)
            continue
        assert state is not None
        states.append(state)
        fingerprints[obs_id] = _fingerprint(conn, obs_id)  # type: ignore[assignment]
    if blockers:
        raise DeliveryBlocked(blockers)

    return {
        "delivery_id": delivery_id,
        "observation_ids": list(observation_ids),
        "items": [
            {
                "observation_id": s["observation_id"],
                "revision_id": s["effective"]["revision_id"],
                "tips": s["tips"],
                "species": s["effective"]["species"],
                "count": s["effective"]["count"],
                "deleted": s["effective"]["deleted"],
                "version": s["version"],
            }
            for s in states
        ],
        "fingerprints": fingerprints,
    }


def replay_existing_delivery(
    conn: sqlite3.Connection, delivery_id: str, observation_ids: list[str]
) -> Optional[dict]:
    """已固化的同号切片：集合相同则返回原切片，集合不同则 409；不存在返回 None。"""
    existing = get_delivery(conn, delivery_id)
    if existing is None:
        return None
    if set(existing["observation_ids"]) != set(observation_ids):
        raise DeliveryConflict(
            "交付号已存在且观测号集合不同，不能覆盖已交付内容",
            delivery_id=existing["delivery_id"],
            delivered_observation_ids=existing["observation_ids"],
        )
    return {**existing, "existing": True}


def commit_delivery(conn: sqlite3.Connection, candidate: dict) -> dict:
    """固化阶段（BEGIN IMMEDIATE 写事务）：一次性发布候选切片。

    写事务跨进程串行化。若组装之后、取到写锁之前有其他进程汇交并改动了
    任一所选观测，指纹不符则整体放弃（409），绝不发布新旧混合的切片。
    并发提交同号时，后到者看到已存在的切片即原样返回，只留一份结果。
    """
    delivery_id = candidate["delivery_id"]
    existing = replay_existing_delivery(
        conn, delivery_id, candidate["observation_ids"]
    )
    if existing is not None:  # 并发同号：先到者已固化
        return existing

    # 组装之后所选观测有任何变化（新修订入库、状态变化）→ 409，不发布混合切片
    changed = [
        obs_id
        for obs_id, fingerprint in candidate["fingerprints"].items()
        if _fingerprint(conn, obs_id) != fingerprint
    ]
    if changed:
        raise DeliveryConflict(
            "所选观测在固化前已变化，本次不发布切片，请重新提交交付",
            delivery_id=delivery_id,
            changed_observation_ids=sorted(changed),
        )

    # 兜底：仍须全部可用（不存在/待补齐/冲突一律阻断）
    blockers: list[dict] = []
    for obs_id in candidate["observation_ids"]:
        blocker = _blocker(conn, obs_id)
        if blocker is not None:
            blockers.append(blocker)
    if blockers:
        raise DeliveryBlocked(blockers)

    conn.execute(
        "INSERT INTO deliveries (delivery_id, observation_ids) VALUES (?,?)",
        (delivery_id, json.dumps(candidate["observation_ids"], ensure_ascii=False)),
    )
    for position, item in enumerate(candidate["items"]):
        conn.execute(
            "INSERT INTO delivery_items (delivery_id, position, observation_id, "
            "revision_id, tips, species, individual_count, is_delete, obs_version) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (
                delivery_id,
                position,
                item["observation_id"],
                item["revision_id"],
                json.dumps(item["tips"], ensure_ascii=False),
                item["species"],
                item["count"],
                1 if item["deleted"] else 0,
                item["version"],
            ),
        )
    return get_delivery(conn, delivery_id)  # type: ignore[return-value]


def _row_to_delivery(row: sqlite3.Row, items: list[dict]) -> dict:
    return {
        "delivery_id": row["delivery_id"],
        "observation_ids": json.loads(row["observation_ids"]),
        "created_at": row["created_at"],
        "items": items,
        "existing": False,
    }


def _item_rows(conn: sqlite3.Connection, delivery_id: str) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM delivery_items WHERE delivery_id = ? ORDER BY position",
        (delivery_id,),
    ).fetchall()
    return [
        {
            "observation_id": r["observation_id"],
            "revision_id": r["revision_id"],
            "tips": json.loads(r["tips"]),
            "species": r["species"],
            "count": r["individual_count"],
            "deleted": bool(r["is_delete"]),
            "version": r["obs_version"],
        }
        for r in rows
    ]


def get_delivery(conn: sqlite3.Connection, delivery_id: str) -> Optional[dict]:
    row = conn.execute(
        "SELECT * FROM deliveries WHERE delivery_id = ?", (delivery_id,)
    ).fetchone()
    if row is None:
        return None
    return _row_to_delivery(row, _item_rows(conn, delivery_id))


def list_deliveries(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM deliveries ORDER BY created_at, delivery_id"
    ).fetchall()
    return [_row_to_delivery(r, _item_rows(conn, r["delivery_id"])) for r in rows]


# ---------------------------------------------------------------- 裁决

def resolve_observation(
    conn: sqlite3.Connection, observation_id: str, rev: RevisionPayload
) -> tuple[dict, bool]:
    """人工裁决：以所见全部分支为父新建修订。返回 (观测状态, 是否幂等重放)。"""
    content = canonical_content(rev)
    hit = conn.execute(
        "SELECT content FROM revisions WHERE revision_id = ?", (rev.revision_id,)
    ).fetchone()
    if hit is not None:  # 同号重传：同内容幂等返回，异内容拒绝
        if hit["content"] != content:
            raise PackageRejected(f"修订 {rev.revision_id} 与库中已有修订同号异内容")
        state = get_observation(conn, observation_id)
        if state is None:
            raise ObservationNotFound(observation_id)
        return state, True

    row = conn.execute(
        "SELECT * FROM observations WHERE observation_id = ?", (observation_id,)
    ).fetchone()
    if row is None:
        raise ObservationNotFound(observation_id)
    current_tips = json.loads(row["tips"])
    if sorted(set(rev.parents)) != current_tips:
        raise BranchesChanged(observation_id, current_tips, row["status"])

    conn.execute(
        "INSERT INTO revisions (revision_id, observation_id, species, individual_count, "
        "is_delete, content) VALUES (?,?,?,?,?,?)",
        (rev.revision_id, observation_id, rev.species, rev.count,
         1 if rev.delete else 0, content),
    )
    for parent in sorted(set(rev.parents)):
        conn.execute(
            "INSERT INTO revision_parents (revision_id, parent_id) VALUES (?,?)",
            (rev.revision_id, parent),
        )
    return recompute_observation(conn, observation_id), False
