"""Tests for the circuit breaker module."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _make_config(daily_limit: float = 5.0, weekly_limit: float = 10.0,
                 resource_floor: float = 10.0) -> MagicMock:
    cfg = MagicMock()
    cfg.globals = {
        "daily_distributed_computecit_limit_base_units": daily_limit,
        "weekly_distributed_computecit_limit_base_units": weekly_limit,
        "resource_hard_floor_base_units": resource_floor,
    }
    cfg.mm = {
        "mm_daily_distributed_computecit_limit_base_units": 6.0,
        "mm_cumulative_distributed_computecit_limit_base_units": 10.0,
    }
    return cfg


class TestDailyExpiry:
    def test_expired_when_past_midnight(self):
        from asynchronous_vector_coordinator_EVC.risk import circuit_breaker as cb
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        with patch.object(cb.db, "get_circuit_state", return_value=yesterday):
            assert cb._is_daily_expired() is True

    def test_not_expired_same_day(self):
        from asynchronous_vector_coordinator_EVC.risk import circuit_breaker as cb
        now = datetime.now(timezone.utc).isoformat()
        with patch.object(cb.db, "get_circuit_state", return_value=now):
            assert cb._is_daily_expired() is False

    def test_expired_when_no_timestamp(self):
        from asynchronous_vector_coordinator_EVC.risk import circuit_breaker as cb
        with patch.object(cb.db, "get_circuit_state", return_value=None):
            assert cb._is_daily_expired() is True

    def test_expired_when_invalid_timestamp(self):
        from asynchronous_vector_coordinator_EVC.risk import circuit_breaker as cb
        with patch.object(cb.db, "get_circuit_state", return_value="not-a-date"):
            assert cb._is_daily_expired() is True


class TestWeeklyExpiry:
    def test_expired_after_7_days(self):
        from asynchronous_vector_coordinator_EVC.risk import circuit_breaker as cb
        eight_days_ago = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()
        with patch.object(cb.db, "get_circuit_state", return_value=eight_days_ago):
            assert cb._is_weekly_expired() is True

    def test_not_expired_within_7_days(self):
        from asynchronous_vector_coordinator_EVC.risk import circuit_breaker as cb
        three_days_ago = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
        with patch.object(cb.db, "get_circuit_state", return_value=three_days_ago):
            assert cb._is_weekly_expired() is False


class TestIsHalted:
    def test_not_halted_when_all_clear(self):
        from asynchronous_vector_coordinator_EVC.risk import circuit_breaker as cb
        with patch.object(cb.db, "get_circuit_state", return_value="0"):
            halted, reason = cb.is_halted()
        assert halted is False
        assert reason is None

    def test_halted_on_manual(self):
        from asynchronous_vector_coordinator_EVC.risk import circuit_breaker as cb

        def _state(key):
            return "1" if key == cb.HALT_MANUAL else "0"

        with patch.object(cb.db, "get_circuit_state", side_effect=_state):
            halted, reason = cb.is_halted()
        assert halted is True
        assert reason == "manual"

    def test_halted_on_resource_floor(self):
        from asynchronous_vector_coordinator_EVC.risk import circuit_breaker as cb

        def _state(key):
            return "1" if key == cb.HALT_CAPITAL else "0"

        with patch.object(cb.db, "get_circuit_state", side_effect=_state):
            halted, reason = cb.is_halted()
        assert halted is True
        assert reason == "resource_floor"


class TestCheckAndTrip:
    def test_trips_daily_limit(self):
        from asynchronous_vector_coordinator_EVC.risk import circuit_breaker as cb

        mock_config = _make_config(daily_limit=5.0)
        with patch.object(cb, "CONFIG", mock_config), \
             patch.object(cb.db, "set_circuit_state", new_callable=AsyncMock) as mock_set:
            result = asyncio.run(cb.check_and_trip(-6.0, -2.0, allocation_base_units=None))
        assert result == "daily_distributed_computecit_limit"

    def test_trips_weekly_limit(self):
        from asynchronous_vector_coordinator_EVC.risk import circuit_breaker as cb

        mock_config = _make_config(weekly_limit=10.0)
        with patch.object(cb, "CONFIG", mock_config), \
             patch.object(cb.db, "set_circuit_state", new_callable=AsyncMock):
            result = asyncio.run(cb.check_and_trip(-2.0, -11.0, allocation_base_units=None))
        assert result == "weekly_distributed_computecit_limit"

    def test_trips_resource_floor(self):
        from asynchronous_vector_coordinator_EVC.risk import circuit_breaker as cb

        mock_config = _make_config(resource_floor=10.0)
        with patch.object(cb, "CONFIG", mock_config), \
             patch.object(cb.db, "set_circuit_state", new_callable=AsyncMock):
            result = asyncio.run(cb.check_and_trip(-1.0, -1.0, allocation_base_units=5.0))
        assert result == "resource_floor_breached"

    def test_no_trip_within_limits(self):
        from asynchronous_vector_coordinator_EVC.risk import circuit_breaker as cb

        mock_config = _make_config()
        with patch.object(cb, "CONFIG", mock_config), \
             patch.object(cb.db, "set_circuit_state", new_callable=AsyncMock):
            result = asyncio.run(cb.check_and_trip(-1.0, -2.0, allocation_base_units=50.0))
        assert result is None


class TestIsMMHalted:
    def test_not_halted_when_clear(self):
        from asynchronous_vector_coordinator_EVC.risk import circuit_breaker as cb
        with patch.object(cb.db, "get_circuit_state", return_value="0"):
            assert cb.is_mm_halted() is False

    def test_halted_on_manual(self):
        from asynchronous_vector_coordinator_EVC.risk import circuit_breaker as cb

        def _state(key):
            return "1" if key == cb.HALT_MANUAL else "0"

        with patch.object(cb.db, "get_circuit_state", side_effect=_state):
            assert cb.is_mm_halted() is True

    def test_halted_on_perm(self):
        from asynchronous_vector_coordinator_EVC.risk import circuit_breaker as cb

        def _state(key):
            return "1" if key == cb.HALT_MM_PERM else "0"

        with patch.object(cb.db, "get_circuit_state", side_effect=_state):
            assert cb.is_mm_halted() is True
