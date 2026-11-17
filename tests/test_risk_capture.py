from decimal import Decimal
from pathlib import Path

import pytest

from dualsettle.risk_capture import load_case


CASE = Path(__file__).resolve().parents[1] / "fixtures" / "risk_capital" / "case.json"


def test_risk_case_keeps_reserve_and_correlated_losses_separate():
    case = load_case(CASE)
    assert case.case_id == "M1-D1-tail-capital-01"
    assert case.run_status == "已归档"
    assert case.stream_net == Decimal("99.00")
    assert case.reserve_amount == Decimal("80.00")
    assert [(s["id"], sum(Decimal(x) for x in s["losses"].values()))
            for s in case.scenarios] == [("ordinary", 0), ("claim_a", 40), ("joint_claim", 90)]
    assert [s["nominal_probability"] for s in case.challenge_scenarios] == ["0.75", "0.15", "0.10"]
    assert case.challenge_bounds[0]["maximum"] == "0.25"
    assert len(case.digest) == 64


def test_risk_case_rejects_outside_manifest(tmp_path):
    outside = tmp_path / "case.json"
    outside.write_bytes(CASE.read_bytes())
    with pytest.raises(ValueError, match="inside repository"):
        load_case(outside)
