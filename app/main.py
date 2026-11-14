"""野外调查观测更正汇交 API。

多台离线设备对同一观测的更正在此汇交：修订 DAG 乱序到达、待补齐自动重算、
多分支冲突、人工裁决、幂等重传、整包拒绝。双进程共库（SQLite WAL）。
交付切片：按调查批次从同一已提交状态固化可复现的观测取数。
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field, model_validator

from . import service
from .db import init_db, read_tx, write_tx


# ------------------------------------------------------------------ 模型

class PayloadFields(BaseModel):
    """修订载荷：物种+数量 或 删除标记，二者互斥。"""

    species: Optional[str] = Field(default=None, min_length=1, max_length=256)
    count: Optional[int] = Field(default=None, ge=0)
    delete: bool = False

    @model_validator(mode="after")
    def _check_payload(self):
        if self.delete:
            if self.species is not None or self.count is not None:
                raise ValueError("删除标记与物种/数量互斥，不能同时携带")
        else:
            if self.species is None or self.count is None:
                raise ValueError("非删除修订必须携带 species 与 count")
        return self


class RevisionIn(PayloadFields):
    observation_id: str = Field(min_length=1, max_length=128)
    revision_id: str = Field(min_length=1, max_length=128)
    parents: list[str] = Field(default_factory=list)


class PackageIn(BaseModel):
    revisions: list[RevisionIn] = Field(min_length=1)


class ResolutionIn(PayloadFields):
    """人工裁决：parents 必须等于裁决时所见全部分支（当前 tips）。"""

    revision_id: str = Field(min_length=1, max_length=128)
    parents: list[str] = Field(default_factory=list)


class EffectiveValue(BaseModel):
    revision_id: str
    species: Optional[str]
    count: Optional[int]
    deleted: bool


class ObservationState(BaseModel):
    observation_id: str
    status: str = Field(description="pending=待补齐 / valid=有效 / conflict=冲突")
    tips: list[str] = Field(description="当前分支（无后继的修订）")
    missing_ancestors: list[str] = Field(description="未齐祖先（待补齐的修订号）")
    effective: Optional[EffectiveValue] = Field(description="有效值；待补齐或冲突时为 null")
    version: int


class PackageResult(BaseModel):
    accepted: list[str] = Field(description="本次新入库的修订号")
    duplicates: list[str] = Field(description="同号同内容幂等跳过的修订号")
    observations: list[ObservationState] = Field(description="受影响观测重算后的状态")


class RevisionOut(BaseModel):
    revision_id: str
    observation_id: str
    parents: list[str]
    species: Optional[str]
    count: Optional[int]
    delete: bool
    received_at: str


class DeliveryIn(BaseModel):
    """交付切片请求：交付号 + 至多 100 个不重复观测号。"""

    delivery_id: str = Field(min_length=1, max_length=128)
    observation_ids: list[str] = Field(min_length=1, max_length=service.MAX_DELIVERY_ITEMS)

    @model_validator(mode="after")
    def _check_unique(self):
        if len(set(self.observation_ids)) != len(self.observation_ids):
            raise ValueError("observation_ids 不能重复")
        if any(not obs.strip() for obs in self.observation_ids):
            raise ValueError("observation_ids 不能包含空字符串")
        return self


class DeliveryItem(BaseModel):
    observation_id: str
    revision_id: str = Field(description="固化时的有效修订号（当前分支）")
    tips: list[str] = Field(description="固化时的当前分支")
    species: Optional[str]
    count: Optional[int]
    deleted: bool
    version: int = Field(description="固化时观测状态版本")


class DeliveryOut(BaseModel):
    delivery_id: str
    observation_ids: list[str]
    created_at: str = Field(description="首次固化时间；同号重试返回原切片时不变")
    items: list[DeliveryItem]
    existing: bool = Field(description="true = 同号重试，返回的是已固化的原切片")


class DeliveryBlocker(BaseModel):
    observation_id: str
    reason: str = Field(description="missing=观测不存在 / pending=待补齐 / conflict=冲突")
    message: str
    status: Optional[str] = None
    tips: Optional[list[str]] = None
    missing_ancestors: Optional[list[str]] = None


class DeliveryBlockedOut(BaseModel):
    message: str
    blockers: list[DeliveryBlocker]


# ------------------------------------------------------------------ 应用

@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="野外调查观测更正汇交 API",
    version="1.1.0",
    description=(
        "汇合多台离线设备对同一观测的更正：修订按 DAG 组织，允许子先父后乱序到达；"
        "祖先未齐标 **pending**（不出有效值），补齐自动重算；单分支为 **valid**；"
        "多分支（含改值与删除并存）为 **conflict**，需人工裁决。"
        "裁决以所见全部分支为父，提交时分支已变返回 409；"
        "同号同内容重传幂等，异内容/循环/跨观测引用整包拒绝。\n\n"
        "**交付切片**：生态分析按调查批次领取可复现的观测数据。"
        "`POST /deliveries` 提交交付号与至多 100 个不重复观测号，从同一已提交状态"
        "读取每项当前分支与有效值；任一观测不存在/待补齐/冲突则返回全部阻断项且"
        "不保存切片，补传后可重试；全部可用才一次性固化观测号、修订号、物种、"
        "数量与删除标记。若所选观测在固化前被其他进程改动则返回 409，不发布混合切片；"
        "同号重试返回原切片，观测号集合不同返回 409；已交付内容不随后续修订变化。"
    ),
    lifespan=lifespan,
)


@app.get("/", include_in_schema=False)
def index():
    return RedirectResponse("/docs")


@app.get("/health", tags=["meta"])
def health():
    return {"status": "ok"}


def _to_payload(r: RevisionIn) -> service.RevisionPayload:
    return service.RevisionPayload(
        observation_id=r.observation_id,
        revision_id=r.revision_id,
        parents=r.parents,
        species=r.species,
        count=r.count,
        delete=r.delete,
    )


@app.post(
    "/deliveries",
    response_model=DeliveryOut,
    status_code=201,
    responses={
        200: {"model": DeliveryOut, "description": "同号同观测号集合重试：返回原切片"},
        409: {
            "description": "交付号已存在但观测号集合不同，或固化前所选观测已变化"
        },
        422: {
            "model": DeliveryBlockedOut,
            "description": "存在阻断项：未保存切片，补传后可重试",
        },
    },
    tags=["交付"],
)
def create_delivery(body: DeliveryIn, response: Response):
    """发布交付切片：交付号 + 至多 100 个不重复观测号。

    组装阶段在只读快照里从同一已提交状态读取全部所选观测；任一观测不存在/
    待补齐/冲突时返回 422 与全部阻断项，不保存切片。全部可用才固化。
    同号重试返回原切片；观测号集合不同或固化前所选观测已变化返回 409。
    """
    try:
        with read_tx() as conn:
            candidate = service.assemble_delivery(
                conn, body.delivery_id, body.observation_ids
            )
    except service.DeliveryBlocked as exc:
        # 并发同号：另一进程可能恰在此验收时固化了切片 → 仍返回原切片
        with read_tx() as conn:
            try:
                replayed = service.replay_existing_delivery(
                    conn, body.delivery_id, body.observation_ids
                )
            except service.DeliveryConflict as conflict:
                raise _delivery_conflict(conflict)
        if replayed is not None:
            response.status_code = 200
            return replayed
        raise HTTPException(
            status_code=422,
            detail={"message": str(exc), "blockers": exc.blockers},
        )
    except service.DeliveryConflict as exc:
        raise _delivery_conflict(exc)

    if candidate.get("existing"):  # 同号重试：不重新组装，原样返回
        response.status_code = 200
        return candidate

    try:
        with write_tx() as conn:
            result = service.commit_delivery(conn, candidate)
    except service.DeliveryBlocked as exc:  # 组装后又被并发汇交置为阻断
        raise HTTPException(
            status_code=422,
            detail={"message": str(exc), "blockers": exc.blockers},
        )
    except service.DeliveryConflict as exc:
        raise _delivery_conflict(exc)
    if result.get("existing"):  # 并发同号：后到者返回先到者的原切片
        response.status_code = 200
    return result


def _delivery_conflict(exc: service.DeliveryConflict) -> HTTPException:
    return HTTPException(
        status_code=409,
        detail={"message": exc.message, "code": "delivery_conflict", **exc.extra},
    )


def _delivery_not_found() -> HTTPException:
    return HTTPException(status_code=404, detail="交付切片不存在")


@app.get("/deliveries", response_model=list[DeliveryOut], tags=["交付"])
def list_deliveries():
    """列出全部已固化交付切片（只含固化内容，不反映后续修订）。"""
    with read_tx() as conn:
        return service.list_deliveries(conn)


@app.get("/deliveries/{delivery_id}", response_model=DeliveryOut, tags=["交付"])
def get_delivery(delivery_id: str):
    """查询已固化交付切片：固化时的观测号、修订号、物种、数量与删除标记。"""
    with read_tx() as conn:
        delivery = service.get_delivery(conn, delivery_id)
    if delivery is None:
        raise _delivery_not_found()
    return delivery


@app.post("/packages", response_model=PackageResult, tags=["汇交"])
def submit_package(pkg: PackageIn):
    """上报一个 JSON 包（可含多条修订、多个观测）。整体原子：任一非法则整包拒绝。"""
    try:
        with write_tx() as conn:
            return service.submit_package(conn, [_to_payload(r) for r in pkg.revisions])
    except service.PackageRejected as exc:
        raise HTTPException(status_code=422, detail=str(exc))


@app.get("/observations", response_model=list[ObservationState], tags=["查询"])
def list_observations(
    status: Optional[str] = Query(default=None, pattern="^(pending|valid|conflict)$")
):
    """列出全部观测状态，可按 status 过滤。"""
    with read_tx() as conn:
        return service.list_observations(conn, status)


@app.get("/conflicts", response_model=list[ObservationState], tags=["查询"])
def list_conflicts():
    """查冲突：返回全部多分支观测及其当前分支（tips），供裁决取数。"""
    with read_tx() as conn:
        return service.list_observations(conn, "conflict")


@app.get("/observations/{observation_id}", response_model=ObservationState, tags=["查询"])
def get_observation(observation_id: str):
    """查单个观测：状态、当前分支、未齐祖先、有效值。"""
    with read_tx() as conn:
        state = service.get_observation(conn, observation_id)
    if state is None:
        raise HTTPException(status_code=404, detail="观测不存在")
    return state


@app.get(
    "/observations/{observation_id}/revisions",
    response_model=list[RevisionOut],
    tags=["查询"],
)
def get_observation_revisions(observation_id: str):
    """查观测的全部修订（DAG 节点）。"""
    with read_tx() as conn:
        state = service.get_observation(conn, observation_id)
        if state is None:
            raise HTTPException(status_code=404, detail="观测不存在")
        return service.list_revisions(conn, observation_id)


@app.post(
    "/observations/{observation_id}/resolutions",
    response_model=ObservationState,
    status_code=201,
    tags=["裁决"],
)
def resolve_observation(observation_id: str, body: ResolutionIn, response: Response):
    """人工裁决：以所见全部分支为父新建修订。分支已变返回 409 及当前分支。"""
    rev = service.RevisionPayload(
        observation_id=observation_id,
        revision_id=body.revision_id,
        parents=body.parents,
        species=body.species,
        count=body.count,
        delete=body.delete,
    )
    try:
        with write_tx() as conn:
            state, replayed = service.resolve_observation(conn, observation_id, rev)
    except service.ObservationNotFound:
        raise HTTPException(status_code=404, detail="观测不存在")
    except service.BranchesChanged as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "提交时分支已变化，请按当前分支重新裁决",
                "current_tips": exc.current_tips,
                "status": exc.status,
            },
        )
    except service.PackageRejected as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    if replayed:  # 同号同内容重传：幂等返回
        response.status_code = 200
    return state
