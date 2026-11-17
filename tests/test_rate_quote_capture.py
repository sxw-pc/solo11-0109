from pathlib import Path

import pytest

from dualsettle.rate_quote_capture import load_case


CASE = Path(__file__).resolve().parents[1] / "fixtures" / "rate_quote" / "case.json"


def test_quote_case_freezes_closed_prior_basket():
    case = load_case(CASE)
    assert case.case_id == "merchant-rate-quote-01"
    assert case.run_status == "已归档"
    assert case.confirmed == (("M1", "T-A", "1.00"), ("M1", "T-B", "1.00"), ("M2", "T-C", "2.00"))
    assert case.frozen_rates == (("M1", "0.0100"), ("M2", "0.0100"))
    assert case.benchmark_fees == (("M1", "0.02"), ("M2", "0.02"))
    assert case.policy["quote_window_id"] == "D2"
    assert len(case.source_digest) == 64


def test_quote_case_rejects_outside_manifest(tmp_path):
    outside = tmp_path / "case.json"
    outside.write_bytes(CASE.read_bytes())
    with pytest.raises(ValueError, match="inside repository"):
        load_case(outside)
