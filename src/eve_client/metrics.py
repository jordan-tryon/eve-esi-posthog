"""Pure metric computation — no I/O."""

from datetime import datetime, timezone

BOUNTY_TYPES = {"bounty_prizes", "ess_escrow_transfer", "bounty_prize", "agent_mission_reward_bonus", "daily_goal_payouts"}
MISSION_TYPES = {"agent_mission_reward", "agent_mission_time_bonus_reward"}
TRADE_TYPES = {
    "market_transaction", "transaction_tax", "brokers_fee", "market_escrow",
    "contract_price", "contract_reward", "contract_price_payment_corp",
}
INDUSTRY_TYPES = {"industry_job_tax", "reprocessing_tax", "industry_job_completed"}

# EVE ship group IDs for capitals
CAPITAL_GROUP_IDS = {
    485,   # Dreadnought
    547,   # Carrier
    659,   # Supercarrier
    30,    # Titan
    1538,  # Force Auxiliary
    883,   # Capital Industrial Ship (Rorqual)
}


def _parse(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def compute_isk_rates(
    journal_entries: list[dict], since: str, until: str
) -> dict:
    """Returns ISK/hr broken down by source category."""
    prev_dt = _parse(since)
    now_dt = _parse(until)
    elapsed_hours = max((now_dt - prev_dt).total_seconds() / 3600, 0.1)

    totals = {"bounty": 0.0, "trade": 0.0, "industry": 0.0, "other": 0.0}

    for e in journal_entries:
        amount = e.get("amount", 0.0)
        if amount <= 0:
            continue
        ref = e.get("ref_type", "")
        if ref in BOUNTY_TYPES or ref in MISSION_TYPES:
            totals["bounty"] += amount
        elif ref in TRADE_TYPES:
            totals["trade"] += amount
        elif ref in INDUSTRY_TYPES:
            totals["industry"] += amount
        else:
            totals["other"] += amount

    total = sum(totals.values())
    return {
        "isk_hour": round(total / elapsed_hours, 2),
        "isk_hour_bounty": round(totals["bounty"] / elapsed_hours, 2),
        "isk_hour_trade": round(totals["trade"] / elapsed_hours, 2),
        "isk_hour_industry": round(totals["industry"] / elapsed_hours, 2),
        "isk_hour_other": round(totals["other"] / elapsed_hours, 2),
    }


def compute_risk_level(
    security_status: float | None,
    ship_group_id: int | None,
    recent_losses: int,
    at_risk_value: float = 0.0,
) -> str:
    if security_status is None:
        level = "HIGH"
    elif security_status >= 0.45:
        level = "LOW"
    elif security_status >= 0.05:
        level = "MEDIUM"
    else:
        level = "HIGH"

    # Escalate for expensive fits in highsec (gank risk)
    if security_status is not None and security_status >= 0.45:
        if at_risk_value >= 60_000_000 and level == "LOW":
            level = "MEDIUM"

    # Escalate for capitals or repeated losses
    if ship_group_id in CAPITAL_GROUP_IDS:
        level = "HIGH"
    elif recent_losses >= 2 and level == "LOW":
        level = "MEDIUM"
    elif recent_losses >= 2 and level == "MEDIUM":
        level = "HIGH"

    return level


def detect_activity_type(journal_entries: list[dict], since: str) -> str:
    since_dt = _parse(since)
    totals = {"Combat PvE": 0.0, "Missions": 0.0, "Trading": 0.0, "Industry": 0.0, "Other": 0.0}

    for e in journal_entries:
        amount = e.get("amount", 0.0)
        if amount <= 0:
            continue
        ref = e.get("ref_type", "")
        if ref in BOUNTY_TYPES:
            totals["Combat PvE"] += amount
        elif ref in MISSION_TYPES:
            totals["Missions"] += amount
        elif ref in TRADE_TYPES:
            totals["Trading"] += amount
        elif ref in INDUSTRY_TYPES:
            totals["Industry"] += amount
        else:
            totals["Other"] += amount

    best = max(totals, key=lambda k: totals[k])
    return best if totals[best] >= 100_000 else "Unknown"
