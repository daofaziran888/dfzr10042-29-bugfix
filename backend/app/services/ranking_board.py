"""排行榜领域服务：分组排行批次 + 指标复核抽核。

排行先按排行维度聚合交易再排名；指标复核只读取分组排行批次里当次真正
入榜的地区，回到该地区的原始交易计算退款和成交笔数，决定本轮抽核对象。
"""
from __future__ import annotations

from typing import Any

from app.models import (
    AggregationMethod,
    DimensionDef,
    MetricDataSource,
    MetricDef,
    MetricPeriod,
    MetricReview,
    RankingBoard,
    SpotCheckRegistry,
)
from app.services.base import BaseModuleService

# 演示用交易明细：同一地区多笔交易、单笔更大但合计不高的地区、同额地区。
# 指标口径固定为「成交金额 SUM，按地区分组」，地区编号用于同额时定序。
SCENARIOS: dict[str, dict[str, Any]] = {
    "west_north": {
        "name": "西区两笔、北区一笔",
        "description": "西区 8 + 7 合计 15，北区单笔 10；按合计取前一名应选西区。",
        "transactions": [
            {"交易编号": "T-W-01", "地区": "西区", "地区编号": "R-02", "成交金额": 8, "退款": 2},
            {"交易编号": "T-W-02", "地区": "西区", "地区编号": "R-02", "成交金额": 7, "退款": 1},
            {"交易编号": "T-N-01", "地区": "北区", "地区编号": "R-03", "成交金额": 10, "退款": 0},
        ],
        "previous_regions": [],
        "quota": 1,
    },
    "west_east_tie": {
        "name": "西区与东区同额",
        "description": "西区与东区当期都是 15；西区是上期抽核地区，东区编号更小。",
        "transactions": [
            {"交易编号": "T-W-01", "地区": "西区", "地区编号": "R-02", "成交金额": 8, "退款": 2},
            {"交易编号": "T-W-02", "地区": "西区", "地区编号": "R-02", "成交金额": 7, "退款": 1},
            {"交易编号": "T-E-01", "地区": "东区", "地区编号": "R-01", "成交金额": 15, "退款": 0},
        ],
        "previous_regions": ["西区"],
        "quota": 1,
    },
}

DEFAULT_SCENARIO = "west_north"

# 抽核登记簿随服务常驻内存；同一轮抽核重复/并发提交只会登记一次。
REGISTRY = SpotCheckRegistry()


def _scenario_payload(scenario: str) -> dict[str, Any]:
    return SCENARIOS.get(scenario) or SCENARIOS[DEFAULT_SCENARIO]


def _ordering(transactions: list[dict[str, Any]], order: str) -> list[dict[str, Any]]:
    rows = [dict(row) for row in transactions]
    if order == "reverse":
        rows.reverse()
    return rows


def _demo_board(entry_id: int, top_n: int = 10) -> RankingBoard:
    metric = MetricDef(
        metric_id="M_TRADE_AMOUNT",
        metric_name="地区成交金额",
        calculation_formula="SUM(成交金额) GROUP BY 地区",
        aggregation=AggregationMethod.SUM,
        data_source=MetricDataSource.DORIS,
        period=MetricPeriod.DAILY,
        owner="data-engineering",
        field_name="成交金额",
        table_name="region_transactions",
        dimension_keys=["地区"],
    )
    dimension = DimensionDef(
        dimension_id="D_REGION",
        dimension_name="地区",
        field_source="地区",
        hierarchy_levels=["地区"],
        enum_cardinality=8,
        linked_metric_count=1,
    )
    return RankingBoard(
        rank_id=f"RANK-{entry_id:04d}",
        rank_metric=metric,
        rank_dimensions=[dimension],
        time_range="当期",
        top_n=top_n,
    )


class RankingBoardService(BaseModuleService):
    MODULE = "ranking_board"
    ENTITY = "排行方案"
    OPERATION_TERM = "计算"
    LIST_FIELDS = ["方案编号", "排行指标", "排行维度", "时间范围", "TOP数量", "更新频率", "最后排行", "方案状态"]
    REQUIRED_FIELDS = ["方案编号", "排行指标", "排行维度"]
    STATUS_ORDER = ["待计算", "计算中", "已生成", "数据过期"]
    ACTION_RULES = {"启动计算": "计算中", "重算排行": "已生成", "标记过期": "数据过期"}
    NEGATIVE_ACTIONS = []

    # ------------------------------------------------------------------
    # 分组排行批次
    # ------------------------------------------------------------------
    def build_ranking(
        self,
        entry_id: int,
        *,
        scenario: str = DEFAULT_SCENARIO,
        order: str = "forward",
        transactions: list[dict[str, Any]] | None = None,
        top_n: int = 10,
    ) -> dict[str, Any]:
        """生成一次分组排行批次；交易顺序不影响批次编号与名次。"""
        entry = self.get_entry(entry_id)
        if entry is None:
            raise KeyError(f"排行方案 {entry_id} 不存在或已归档")
        config = _scenario_payload(scenario)
        source_rows = transactions if transactions is not None else config["transactions"]
        rows = _ordering(source_rows, order)

        board = _demo_board(entry_id, top_n=max(int(top_n), 1))
        batch = board.build_batch(rows, code_field="地区编号")
        return {
            "rank_id": board.rank_id,
            "scenario": scenario,
            "scenario_name": config["name"],
            "order": order,
            "distance_rows": len(rows),
            "batch": batch.to_dict(),
        }

    # ------------------------------------------------------------------
    # 指标复核 / 抽核
    # ------------------------------------------------------------------
    def build_review(
        self,
        entry_id: int,
        *,
        scenario: str = DEFAULT_SCENARIO,
        order: str = "forward",
        quota: int | None = None,
        previous_regions: list[str] | None = None,
        transactions: list[dict[str, Any]] | None = None,
        register: bool = True,
    ) -> dict[str, Any]:
        entry = self.get_entry(entry_id)
        if entry is None:
            raise KeyError(f"排行方案 {entry_id} 不存在或已归档")
        config = _scenario_payload(scenario)
        source_rows = transactions if transactions is not None else config["transactions"]
        rows = _ordering(source_rows, order)

        board = _demo_board(entry_id)
        batch = board.build_batch(rows, code_field="地区编号")
        review = MetricReview(
            batch=batch,
            transactions=rows,
            quota=int(config.get("quota", 1) if quota is None else quota),
            previous_regions=(
                list(previous_regions)
                if previous_regions is not None
                else list(config.get("previous_regions", []))
            ),
            region_field="地区",
            code_field="地区编号",
            refund_field="退款",
            transaction_id_field="交易编号",
        )
        report = review.build()

        first_registration = False
        if register:
            report, first_registration = REGISTRY.register(report)
        return {
            "rank_id": board.rank_id,
            "scenario": scenario,
            "scenario_name": config["name"],
            "order": order,
            "distance_rows": len(rows),
            "report": report,
            "first_registration": first_registration,
        }

    def list_reviews(self, entry_id: int) -> list[dict[str, Any]]:
        prefix = f"RANK-{entry_id:04d}-"
        return [
            record for record in REGISTRY.records()
            if str(record.get("batch_id", "")).startswith(prefix)
        ]

    def approve_review(self, entry_id: int, round_key: str) -> dict[str, Any]:
        record = REGISTRY.approve(round_key)
        if record is None or not str(record.get("batch_id", "")).startswith(
            f"RANK-{entry_id:04d}-"
        ):
            raise KeyError(f"抽核轮次 {round_key} 不存在")
        return record

    def recompute_review(
        self,
        entry_id: int,
        round_key: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        kwargs.pop("register", None)
        result = self.build_review(entry_id, register=False, **kwargs)
        recomputed = REGISTRY.recompute(round_key, result["report"])
        if recomputed is None:
            raise KeyError(f"抽核轮次 {round_key} 不存在")
        return {"recomputed": recomputed, "report": result["report"]}
