"""排行榜专项接口：分组排行批次、指标复核与抽核登记。

通用资源路由只负责清单和动作；排行/复核对交易明细做计算，单独挂在这里，
避免把业务计算塞进路由层。路径都在 /api/ranking_board 下，注册顺序早于
通用 /api/{module}/{entry_id}，防止固定子路径被动态参数抢先匹配。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.services.ranking_board import (
    DEFAULT_SCENARIO,
    SCENARIOS,
    RankingBoardService,
)

router = APIRouter(prefix="/api/ranking_board", tags=["排行榜"])

service = RankingBoardService()


class ReviewPayload(BaseModel):
    scenario: str = DEFAULT_SCENARIO
    order: str = "forward"
    quota: int | None = None
    previous_regions: list[str] | None = None
    transactions: list[dict[str, Any]] | None = None
    register: bool = True


@router.get("/scenarios", response_model=dict)
def list_scenarios() -> dict[str, Any]:
    """列出内置复核场景，方便页面上切换西区/北区、西区/东区两组数据。"""
    return {
        "default": DEFAULT_SCENARIO,
        "items": [
            {
                "key": key,
                "name": value["name"],
                "description": value["description"],
                "quota": value.get("quota", 1),
                "previous_regions": value.get("previous_regions", []),
                "transaction_count": len(value["transactions"]),
            }
            for key, value in SCENARIOS.items()
        ],
    }


@router.get("/{entry_id}/ranking", response_model=dict)
def compute_ranking(
    entry_id: int,
    scenario: str = Query(default=DEFAULT_SCENARIO),
    order: str = Query(default="forward"),
    top_n: int = Query(default=10, ge=1, le=200),
) -> dict[str, Any]:
    """生成分组排行批次：先按地区聚合成交金额，再对地区排名。"""
    try:
        return service.build_ranking(entry_id, scenario=scenario, order=order, top_n=top_n)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc.args[0])) from exc


@router.post("/{entry_id}/review", response_model=dict)
def build_review(entry_id: int, payload: ReviewPayload) -> dict[str, Any]:
    """按当次排行批次生成指标复核报告，并登记本轮抽核。"""
    try:
        return service.build_review(entry_id, **payload.model_dump())
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc.args[0])) from exc


@router.get("/{entry_id}/reviews", response_model=dict)
def list_reviews(entry_id: int) -> dict[str, Any]:
    return {"items": service.list_reviews(entry_id)}


@router.post("/{entry_id}/reviews/{round_key}/approve", response_model=dict)
def approve_review(entry_id: int, round_key: str) -> dict[str, Any]:
    try:
        return service.approve_review(entry_id, round_key)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc.args[0])) from exc


@router.post("/{entry_id}/reviews/{round_key}/recompute", response_model=dict)
def recompute_review(entry_id: int, round_key: str, payload: ReviewPayload) -> dict[str, Any]:
    """已审定抽核可复算：比对结果是否漂移，不改写已审定记录。"""
    try:
        return service.recompute_review(
            entry_id, round_key, **{**payload.model_dump(), "register": False}
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc.args[0])) from exc
