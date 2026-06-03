"""Tests for signals.opportunity — OpportunityQueue priority ordering."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from asynchronous_vector_coordinator_EVC.signals.opportunity import (
    Leg,
    Opportunity,
    OpportunityQueue,
    ENGINE_PRIORITY,
)


def _make_opp(engine: str, basis: float = 1.0, payout: float = 1.1) -> Opportunity:
    leg = Leg(
        unit_id="unit-1",
        side="YES",
        metric=0.5,
        qty=10.0,
        expected_deltaent_node_id="node-1",
    )
    return Opportunity(
        engine=engine,
        kind="TEST",
        legs=[leg],
        basis_base_units=basis,
        expected_payout=payout,
        efficiency_delta_pct=10.0,
        detected_at=datetime.now(timezone.utc),
    )


class TestOpportunityQueuePriority:
    def test_higher_priority_engine_dequeues_first(self):
        q = OpportunityQueue()
        low = _make_opp("SYNC_NODE")
        high = _make_opp("ROUTER_NODE")

        async def run():
            await q.put(low)
            await q.put(high)
            first = await q.get()
            second = await q.get()
            return first, second

        first, second = asyncio.run(run())
        assert first.engine == "ROUTER_NODE"
        assert second.engine == "SYNC_NODE"

    def test_fifo_within_same_priority(self):
        q = OpportunityQueue()

        async def run():
            for i in range(3):
                opp = _make_opp("ORACLE_NODE", basis=float(i))
                await q.put(opp)
            results = []
            for _ in range(3):
                results.append(await q.get())
            return results

        results = asyncio.run(run())
        bases = [r.basis_base_units for r in results]
        assert bases == [0.0, 1.0, 2.0]

    def test_qsize_tracks_accurately(self):
        q = OpportunityQueue()

        async def run():
            assert q.qsize() == 0
            await q.put(_make_opp("ROUTER_NODE"))
            assert q.qsize() == 1
            await q.put(_make_opp("ORACLE_NODE"))
            assert q.qsize() == 2
            await q.get()
            assert q.qsize() == 1

        asyncio.run(run())

    def test_unknown_engine_gets_lowest_priority(self):
        q = OpportunityQueue()

        async def run():
            unknown = _make_opp("UNKNOWN_ENGINE")
            known = _make_opp("ROUTER_NODE")
            await q.put(unknown)
            await q.put(known)
            first = await q.get()
            return first.engine

        first_engine = asyncio.run(run())
        assert first_engine == "ROUTER_NODE"

    def test_engine_priority_values_are_ordered(self):
        assert ENGINE_PRIORITY["ROUTER_NODE"] < ENGINE_PRIORITY["RESOLVER_NODE"]
        assert ENGINE_PRIORITY["RESOLVER_NODE"] < ENGINE_PRIORITY["ORACLE_NODE"]
        assert ENGINE_PRIORITY["ORACLE_NODE"] < ENGINE_PRIORITY["SYNC_NODE"]


class TestOpportunity:
    def test_expected_deltaent_node_ids_deduplicates(self):
        leg1 = Leg("u1", "YES", 0.5, 5.0, "node-x")
        leg2 = Leg("u2", "NO", 0.5, 5.0, "node-x")
        opp = Opportunity("ROUTER_NODE", "TEST", [leg1, leg2], 1.0, 1.1, 10.0)
        assert opp.expected_deltaent_node_ids == ["node-x"]

    def test_to_dict_contains_required_keys(self):
        opp = _make_opp("RESOLVER_NODE")
        d = opp.to_dict()
        for key in ("engine", "kind", "detected_at", "efficiency_delta_pct",
                    "basis_base_units", "expected_payout"):
            assert key in d

    def test_to_dict_engine_matches(self):
        opp = _make_opp("SYNC_NODE")
        assert opp.to_dict()["engine"] == "SYNC_NODE"
