"""Freeze a settled reserve and correlated scenarios; no capital verdict yet."""
from __future__ import annotations

import hashlib
import json
import sys
import tempfile
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from .__main__ import demo
from .payout import open_reserve, reserve_exposure
from .store import connect


@dataclass(frozen=True)
class RiskCase:
    case_id: str
    digest: str
    policy: dict
    scenarios: tuple[dict, ...]
    coupled_bounds: tuple[dict, ...]
    challenge_scenarios: tuple[dict, ...]
    challenge_bounds: tuple[dict, ...]
    reserve_amount: Decimal
    stream_net: Decimal
    run_status: str


def _inside(root: Path, relative: str) -> Path:
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise ValueError("risk source must be relative")
    path = (root / relative).resolve()
    if not path.is_relative_to(root) or not path.is_file() or path.is_symlink():
        raise ValueError(f"risk source unavailable: {relative}")
    return path


def _money(raw: str) -> Decimal:
    number = Decimal(raw)
    if not number.is_finite() or number < 0 or number != number.quantize(Decimal("0.01")):
        raise ValueError("invalid scenario money")
    return number


def load_case(manifest: str | Path) -> RiskCase:
    root = Path(__file__).resolve().parents[2]
    path = Path(manifest).resolve()
    if not path.is_relative_to(root) or not path.is_file():
        raise ValueError("risk manifest must be inside repository")
    raw = json.loads(path.read_text(encoding="utf-8"))
    if set(raw) != {"policy", "scenarios", "challenge", "sha256"}:
        raise ValueError("risk manifest fields differ")
    names = {raw["policy"], raw["scenarios"], raw["challenge"]}
    if len(names) != 3 or set(raw["sha256"]) != names:
        raise ValueError("risk digest coverage differs")
    paths = {name: _inside(root, name) for name in names}
    for name, source in paths.items():
        if hashlib.sha256(source.read_bytes()).hexdigest() != raw["sha256"][name]:
            raise ValueError(f"risk source changed: {name}")
    policy = json.loads(paths[raw["policy"]].read_text(encoding="utf-8"))
    scenarios_doc = json.loads(paths[raw["scenarios"]].read_text(encoding="utf-8"))
    challenge_doc = json.loads(paths[raw["challenge"]].read_text(encoding="utf-8"))
    if set(policy) != {"case_id", "merchant", "window_id", "alpha", "reserve_cases",
                       "contracts", "capital_scope", "risk_unit"} or \
       set(scenarios_doc) != {"scenarios", "coupled_probability_bounds"} or \
       set(challenge_doc) != {"scenarios", "coupled_probability_bounds"}:
        raise ValueError("risk policy or scenario fields differ")
    alpha = Decimal(policy["alpha"])
    if not 0 < alpha < 1:
        raise ValueError("invalid risk tail")
    reserves = policy["reserve_cases"]
    case_ids = {r["case_id"] for r in reserves}
    if len(case_ids) != len(reserves) or not case_ids:
        raise ValueError("risk reserve case identity differs")
    for reserve in reserves:
        if _money(reserve["amount"]) <= 0:
            raise ValueError("risk reserve must be positive")
    scenarios = scenarios_doc["scenarios"]
    scenario_ids = {s["id"] for s in scenarios}
    if len(scenario_ids) != len(scenarios) or not scenarios:
        raise ValueError("risk scenario identity differs")
    nominal_sum = Decimal(0)
    for scenario in scenarios:
        if set(scenario["losses"]) != case_ids:
            raise ValueError("risk loss case coverage differs")
        for amount in scenario["losses"].values():
            _money(amount)
        low = Decimal(scenario["probability_lower"])
        nominal = Decimal(scenario["nominal_probability"])
        high = Decimal(scenario["probability_upper"])
        if not 0 <= low <= nominal <= high <= 1:
            raise ValueError("invalid scenario probability interval")
        nominal_sum += nominal
    if nominal_sum != 1:
        raise ValueError("nominal probabilities do not sum to one")
    for bound in scenarios_doc["coupled_probability_bounds"]:
        ids = bound["scenario_ids"]
        if not ids or len(set(ids)) != len(ids) or not set(ids) <= scenario_ids:
            raise ValueError("coupled scenario identities differ")
        low, high = Decimal(bound["minimum"]), Decimal(bound["maximum"])
        nominal = sum((Decimal(s["nominal_probability"]) for s in scenarios
                       if s["id"] in ids), Decimal(0))
        if not 0 <= low <= nominal <= high <= 1:
            raise ValueError("nominal distribution violates coupled bound")
    challenge = challenge_doc["scenarios"]
    if {s["id"] for s in challenge} != scenario_ids or \
       any(set(s["losses"]) != case_ids for s in challenge) or \
       sum((Decimal(s["nominal_probability"]) for s in challenge), Decimal(0)) != 1:
        raise ValueError("challenge scenarios differ")
    for scenario in challenge:
        for amount in scenario["losses"].values():
            _money(amount)
        low, nominal, high = (Decimal(scenario[key]) for key in
                              ("probability_lower", "nominal_probability", "probability_upper"))
        if not 0 <= low <= nominal <= high <= 1:
            raise ValueError("challenge probability interval differs")
    for bound in challenge_doc["coupled_probability_bounds"]:
        ids = bound["scenario_ids"]
        nominal = sum((Decimal(s["nominal_probability"]) for s in challenge
                       if s["id"] in ids), Decimal(0))
        if not ids or len(set(ids)) != len(ids) or not set(ids) <= scenario_ids or \
           not 0 <= Decimal(bound["minimum"]) <= nominal <= Decimal(bound["maximum"]) <= 1:
            raise ValueError("challenge coupled bound differs")
    with tempfile.TemporaryDirectory(prefix="dualsettle-risk-") as folder:
        temporary = Path(folder)
        demo(temporary)
        db_path = temporary / "data.db"
        for reserve in reserves:
            open_reserve(db_path, reserve["case_id"], policy["merchant"],
                         policy["window_id"], reserve["amount"], reserve["reason"])
        db = connect(db_path)
        try:
            run = db.execute("SELECT id,status FROM reconcile_run ORDER BY id DESC LIMIT 1").fetchone()
            verdict = db.execute("SELECT stream_net FROM verdict WHERE run_id=? AND merchant=?",
                                 (run["id"], policy["merchant"])).fetchone()
            if run["status"] != "已归档" or verdict is None:
                raise ValueError("risk case requires frozen settled merchant")
            reserve_amount, _ = reserve_exposure(db, policy["merchant"], policy["window_id"])
            stream_net = Decimal(verdict["stream_net"])
        finally:
            db.close()
    digest = hashlib.sha256(json.dumps(raw, sort_keys=True,
                                       separators=(",", ":")).encode("utf-8")).hexdigest()
    return RiskCase(policy["case_id"], digest, policy, tuple(scenarios),
                    tuple(scenarios_doc["coupled_probability_bounds"]),
                    tuple(challenge), tuple(challenge_doc["coupled_probability_bounds"]),
                    reserve_amount, stream_net, run["status"])


def main() -> int:
    default = Path(__file__).resolve().parents[2] / "fixtures" / "risk_capital" / "case.json"
    case = load_case(sys.argv[1] if len(sys.argv) > 1 else default)
    print(json.dumps({"case_id": case.case_id, "digest": case.digest,
                      "merchant": case.policy["merchant"], "run_status": case.run_status,
                      "stream_net": str(case.stream_net), "open_reserve": str(case.reserve_amount),
                      "challenge_scenarios": len(case.challenge_scenarios),
                      "scenario_losses": {s["id"]: str(sum((_money(x) for x in s["losses"].values()), Decimal(0)))
                                          for s in case.scenarios}}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
