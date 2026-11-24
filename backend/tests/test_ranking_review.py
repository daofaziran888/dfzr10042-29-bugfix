"""排行榜分组排行与指标复核测试

覆盖本次修复的核心口径：
1. 排行先按指标定义聚合分组，再排名（西区 8+7 合计 15 胜过北区单笔 10）；
2. 指标复核从分组排行批次取真正入榜的地区，按原交易算退款和成交笔数；
3. 同额时上期抽核地区让位、编号更小者优先；
4. 换输入顺序重算不漂移、并发/重复抽核只登记一次、已审定可复算；
5. 未入榜地区不进入报告、不贡献退款。
"""
from __future__ import annotations

import threading

import pytest

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


def _metric() -> MetricDef:
    return MetricDef(
        metric_id="M_AMT",
        metric_name="地区成交金额",
        calculation_formula="SUM(成交金额) GROUP BY 地区",
        aggregation=AggregationMethod.SUM,
        data_source=MetricDataSource.DORIS,
        period=MetricPeriod.DAILY,
        owner="analyst",
        field_name="成交金额",
        table_name="region_transactions",
        dimension_keys=["地区"],
    )


def _board(top_n: int = 10) -> RankingBoard:
    return RankingBoard(
        rank_id="RANK_TEST",
        rank_metric=_metric(),
        rank_dimensions=[
            DimensionDef(
                dimension_id="D_REGION",
                dimension_name="地区",
                field_source="地区",
                hierarchy_levels=["地区"],
            )
        ],
        top_n=top_n,
    )


@pytest.fixture
def west_north_rows():
    return [
        {"交易编号": "T1", "地区": "西区", "地区编号": "R-02", "成交金额": 8, "退款": 2},
        {"交易编号": "T2", "地区": "西区", "地区编号": "R-02", "成交金额": 7, "退款": 1},
        {"交易编号": "T3", "地区": "北区", "地区编号": "R-03", "成交金额": 10, "退款": 0},
    ]


class TestGroupedRanking:
    def test_groups_same_region_before_ranking(self, west_north_rows):
        entries = _board().compute_ranking(west_north_rows, code_field="地区编号")
        assert [(item["地区"], item["成交金额"]) for item in entries] == [
            ("西区", 15.0),
            ("北区", 10.0),
        ]

    def test_input_order_does_not_change_ranking(self, west_north_rows):
        forward = _board().compute_ranking(west_north_rows, code_field="地区编号")
        reverse = _board().compute_ranking(list(reversed(west_north_rows)), code_field="地区编号")
        assert forward == reverse

    def test_single_large_row_does_not_beat_aggregated_region(self, west_north_rows):
        entries = _board(top_n=1).compute_ranking(west_north_rows, code_field="地区编号")
        assert [item["地区"] for item in entries] == ["西区"]
        assert "北区" not in [item["地区"] for item in entries]

    def test_batch_id_is_stable_under_reordering(self, west_north_rows):
        forward = _board().build_batch(west_north_rows, code_field="地区编号")
        reverse = _board().build_batch(list(reversed(west_north_rows)), code_field="地区编号")
        assert forward.batch_id == reverse.batch_id
        assert forward.digest == reverse.digest
        assert forward.entries == reverse.entries

    def test_pipeline_batch_feeds_review(self, west_north_rows):
        from app.analysis_pipeline import pipeline_ranking_to_metric

        result = pipeline_ranking_to_metric(
            [_board()],
            {"RANK_TEST": west_north_rows},
            code_fields={"RANK_TEST": "地区编号"},
        )
        batch = result["rankings"][0]["batch"]
        assert batch["regions"] == ["西区", "北区"]
        report = MetricReview(
            _board().build_batch(west_north_rows, code_field="地区编号"),
            west_north_rows,
            quota=1,
        ).build()
        assert report["batch_id"] == batch["batch_id"]
        assert report["target"] == "西区"


class TestMetricReview:
    def test_review_reads_batch_and_original_transactions(self, west_north_rows):
        board = _board()
        report = MetricReview(
            board.build_batch(west_north_rows, code_field="地区编号"),
            west_north_rows,
            quota=1,
        ).build()
        selected = next(item for item in report["regions"] if item["selected"])
        assert selected["region"] == "西区"
        assert selected["value"] == 15.0
        assert selected["transaction_count"] == 2
        assert selected["refund"] == 3.0
        assert report["total_refund"] == 3.0

    def test_review_is_stable_when_transaction_order_changes(self, west_north_rows):
        board = _board()
        forward = MetricReview(
            board.build_batch(west_north_rows, code_field="地区编号"), west_north_rows, quota=1
        ).build()
        reverse = MetricReview(
            board.build_batch(list(reversed(west_north_rows)), code_field="地区编号"),
            list(reversed(west_north_rows)),
            quota=1,
        ).build()
        assert forward["digest"] == reverse["digest"]
        assert forward["target"] == reverse["target"] == "西区"

    def test_tie_prefers_region_not_audited_last_round(self):
        rows = [
            {"交易编号": "T1", "地区": "西区", "地区编号": "R-02", "成交金额": 8, "退款": 2},
            {"交易编号": "T2", "地区": "西区", "地区编号": "R-02", "成交金额": 7, "退款": 1},
            {"交易编号": "T3", "地区": "东区", "地区编号": "R-01", "成交金额": 15, "退款": 0},
        ]
        report = MetricReview(
            _board().build_batch(rows, code_field="地区编号"),
            rows,
            quota=1,
            previous_regions=["西区"],
        ).build()
        assert report["selected"] == ["东区"]
        assert report["target"] == "东区"
        assert report["total_refund"] == 0.0

    def test_regions_outside_batch_cannot_contribute(self, west_north_rows):
        # 南区不在排行批次里，交易明细再多也不能进报告、不能贡献退款。
        rows = west_north_rows + [
            {"交易编号": "T4", "地区": "南区", "地区编号": "R-09", "成交金额": 99, "退款": 50},
        ]
        report = MetricReview(
            _board().build_batch(west_north_rows, code_field="地区编号"),
            rows,
            quota=5,
        ).build()
        assert "南区" not in [item["region"] for item in report["regions"]]
        assert report["target"] == "西区"
        assert report["total_refund"] == 3.0


class TestSpotCheckRegistry:
    def _report(self) -> dict:
        return {"round_key": "REV-TEST", "target": "西区", "digest": "abc"}

    def test_concurrent_register_only_once(self):
        registry = SpotCheckRegistry()
        results: list[bool] = []
        barrier = threading.Barrier(8)

        def worker() -> None:
            barrier.wait()
            _, created = registry.register(self._report())
            results.append(created)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert results.count(True) == 1
        assert len(registry.records()) == 1

    def test_duplicate_submission_returns_first_record(self):
        registry = SpotCheckRegistry()
        first, created = registry.register(self._report())
        second, created_again = registry.register({**self._report(), "target": "东区"})
        assert created is True
        assert created_again is False
        assert second["target"] == first["target"] == "西区"

    def test_approved_review_can_be_recomputed_without_drift(self):
        registry = SpotCheckRegistry()
        report, _ = registry.register(self._report())
        approved = registry.approve(report["round_key"])
        assert approved is not None and approved["status"] == "已审定"

        same = registry.recompute(report["round_key"], self._report())
        assert same is not None and same["matches"] is True

        drifted = registry.recompute(report["round_key"], {**self._report(), "digest": "zzz"})
        assert drifted is not None and drifted["matches"] is False
        # 复算只做核对，不改写已审定记录。
        assert registry.get(report["round_key"])["status"] == "已审定"
