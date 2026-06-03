"""Tests for common.execution_costs — fee calculation logic."""

from __future__ import annotations

import pytest

from asynchronous_vector_coordinator_EVC.common.execution_costs import (
    classify_expected_deltaent_node_category,
    consumer_gas_cost_pct,
    net_efficiency_delta_pct,
    provider_rebate_base_units,
    MAKER_REBATE_PCT,
)


class TestClassifyCategory:
    def test_geopolitics_tag(self):
        assert classify_expected_deltaent_node_category(["geopolitics"]) == "geopolitics"

    def test_governance_tag(self):
        assert classify_expected_deltaent_node_category(["governance"]) == "governance"

    def test_technology_tag(self):
        assert classify_expected_deltaent_node_category(["technology"]) == "technology"

    def test_unknown_tag_returns_default(self):
        assert classify_expected_deltaent_node_category(["unknowntag123"]) == "default"

    def test_empty_tags_returns_default(self):
        assert classify_expected_deltaent_node_category([]) == "default"

    def test_none_tags_returns_default(self):
        assert classify_expected_deltaent_node_category(None) == "default"

    def test_case_insensitive(self):
        assert classify_expected_deltaent_node_category(["GEOPOLITICS"]) == "geopolitics"

    def test_first_match_wins(self):
        result = classify_expected_deltaent_node_category(["geopolitics", "governance"])
        assert result in ("geopolitics", "governance")


class TestConsumerGasCostPct:
    def test_zero_at_boundary_mid_zero(self):
        assert consumer_gas_cost_pct("geopolitics", 0.0) == 0.0

    def test_zero_at_boundary_mid_one(self):
        assert consumer_gas_cost_pct("geopolitics", 1.0) == 0.0

    def test_peak_at_mid_half(self):
        pct = consumer_gas_cost_pct("technology", 0.5)
        assert pct > 0.0

    def test_geopolitics_always_zero(self):
        for mid in (0.1, 0.3, 0.5, 0.7, 0.9):
            assert consumer_gas_cost_pct("geopolitics", mid) == 0.0

    def test_symmetry_around_half(self):
        cost_low = consumer_gas_cost_pct("technology", 0.3)
        cost_high = consumer_gas_cost_pct("technology", 0.7)
        assert abs(cost_low - cost_high) < 1e-9


class TestNetEfficiencyDeltaPct:
    def test_positive_when_payout_exceeds_basis(self):
        result = net_efficiency_delta_pct(1.0, 0.90)
        assert result > 0.0

    def test_negative_when_payout_below_basis(self):
        result = net_efficiency_delta_pct(0.80, 1.0)
        assert result < 0.0

    def test_returns_minus_100_on_zero_basis(self):
        assert net_efficiency_delta_pct(1.0, 0.0) == -100.0

    def test_category_affects_result(self):
        no_fee = net_efficiency_delta_pct(1.0, 0.95, category="geopolitics", mid_metric=0.5)
        with_fee = net_efficiency_delta_pct(1.0, 0.95, category="technology", mid_metric=0.5)
        assert no_fee >= with_fee


class TestProviderRebate:
    def test_zero_input_gives_zero(self):
        assert provider_rebate_base_units(0.0) == 0.0

    def test_negative_input_gives_zero(self):
        assert provider_rebate_base_units(-1.0) == 0.0

    def test_rebate_fraction_of_input(self):
        result = provider_rebate_base_units(10.0)
        assert abs(result - 10.0 * MAKER_REBATE_PCT) < 1e-9
