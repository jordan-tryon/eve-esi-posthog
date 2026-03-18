"""Pure metric computation — no I/O."""

from datetime import datetime, timezone

# True ISK faucets — NPC-sourced, new ISK entering the economy
BOUNTY_TYPES = {"bounty_prizes", "ess_escrow_transfer", "bounty_prize"}
MISSION_TYPES = {
    "agent_mission_reward", "agent_mission_time_bonus_reward",
    "agent_mission_reward_bonus", "daily_goal_payouts",
}
# ISK exchanges — move existing ISK between players, do NOT create it
TRADE_TYPES = {
    "market_transaction", "transaction_tax", "brokers_fee", "market_escrow",
}
INDUSTRY_TYPES = {"industry_job_tax", "reprocessing_tax", "industry_job_completed"}

# Escrow returns — positive journal entries that are just YOUR OWN ISK coming back from escrow.
# Must be excluded from session income calculations or they'll inflate earnings.
ESCROW_RETURN_TYPES = {
    "contract_collateral_refund",    # you delivered successfully, collateral returned
    "contract_reward_refund",         # courier failed, poster's reward returned to them
    "contract_auction_bid_refund",    # your auction bid was outbid
    "contract_deposit_refund",        # contract completed/cancelled, creation deposit returned
}

# Contract income — ISK you actually earned from contract activity (player-to-player, not faucets)
CONTRACT_INCOME_TYPES = {
    "contract_reward",               # you completed a courier/hauling job
    "contract_price",                # item exchange — you sold items
    "contract_price_payment_corp",   # corp version
    "contract_auction_sold",         # your auction contract won
    "contract_collateral_payout",    # courier failed — you received their collateral
}

_NON_FAUCET_TYPES = TRADE_TYPES | INDUSTRY_TYPES | ESCROW_RETURN_TYPES | CONTRACT_INCOME_TYPES

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
    """Returns ISK/hr broken down by true faucet source category.

    Trading and industry are ISK exchanges, not ISK creation — they are
    excluded from this breakdown. The isk_hour_trade column is repurposed
    to hold mission/event income.
    """
    prev_dt = _parse(since)
    now_dt = _parse(until)
    elapsed_hours = max((now_dt - prev_dt).total_seconds() / 3600, 0.1)

    totals = {"combat": 0.0, "missions": 0.0, "other": 0.0}

    for e in journal_entries:
        amount = e.get("amount", 0.0)
        if amount <= 0:
            continue
        ref = e.get("ref_type", "")
        if ref in _NON_FAUCET_TYPES:
            continue  # ISK exchange, not creation
        if ref in BOUNTY_TYPES:
            totals["combat"] += amount
        elif ref in MISSION_TYPES:
            totals["missions"] += amount
        else:
            totals["other"] += amount

    total = sum(totals.values())
    return {
        "isk_hour": round(total / elapsed_hours, 2),
        "isk_hour_bounty": round(totals["combat"] / elapsed_hours, 2),    # Combat
        "isk_hour_trade": round(totals["missions"] / elapsed_hours, 2),   # Missions (column repurposed)
        "isk_hour_industry": 0.0,                                          # Unused — industry is not a faucet
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
    """Detect primary activity from true ISK faucet entries only.
    Trading and industry are excluded — they don't reveal ISK-generating activity."""
    totals = {"Combat PvE": 0.0, "Missions": 0.0, "Other": 0.0}

    for e in journal_entries:
        amount = e.get("amount", 0.0)
        if amount <= 0:
            continue
        ref = e.get("ref_type", "")
        if ref in _NON_FAUCET_TYPES:
            continue
        if ref in BOUNTY_TYPES:
            totals["Combat PvE"] += amount
        elif ref in MISSION_TYPES:
            totals["Missions"] += amount
        else:
            totals["Other"] += amount

    best = max(totals, key=lambda k: totals[k])
    return best if totals[best] >= 100_000 else "Unknown"


def compute_trade_pnl(transactions: list[dict], journal_entries: list[dict]) -> list[dict]:
    """
    FIFO P&L over the supplied transaction window.
    Returns list of dicts:
      {type_id, type_name, qty_bought, qty_sold, avg_buy, avg_sell,
       gross_profit, fees, net_profit, margin_pct}
    """
    from collections import deque

    # Total fee amounts from journal (brokers_fee + transaction_tax), all positive
    total_fees = sum(
        abs(e.get("amount", 0.0))
        for e in journal_entries
        if e.get("ref_type") in ("brokers_fee", "transaction_tax")
    )

    # Group transactions by type_id
    by_type: dict[int, list[dict]] = {}
    for tx in transactions:
        tid = tx["type_id"]
        by_type.setdefault(tid, []).append(tx)

    # Total sell volume for fee allocation denominator
    total_sell_isk = sum(
        tx["unit_price"] * tx["quantity"]
        for tx in transactions
        if not tx.get("is_buy")
    )

    results = []
    for type_id, txs in by_type.items():
        type_name = next((t.get("type_name") for t in txs if t.get("type_name")), str(type_id))
        buys  = sorted([t for t in txs if t.get("is_buy")],  key=lambda t: t["date"])
        sells = sorted([t for t in txs if not t.get("is_buy")], key=lambda t: t["date"])

        if not sells:
            continue

        buy_queue: deque[tuple[int, float]] = deque()
        for b in buys:
            buy_queue.append((b["quantity"], b["unit_price"]))

        qty_bought = sum(b["quantity"] for b in buys)
        qty_sold   = sum(s["quantity"] for s in sells)
        total_sell_isk_type = sum(s["unit_price"] * s["quantity"] for s in sells)
        avg_sell = total_sell_isk_type / qty_sold if qty_sold else 0.0

        gross_profit = 0.0
        matched_buy_isk = 0.0
        matched_qty = 0

        for s in sells:
            remaining = s["quantity"]
            while remaining > 0 and buy_queue:
                bqty, bprice = buy_queue[0]
                take = min(remaining, bqty)
                gross_profit += (s["unit_price"] - bprice) * take
                matched_buy_isk += bprice * take
                matched_qty += take
                remaining -= take
                if take == bqty:
                    buy_queue.popleft()
                else:
                    buy_queue[0] = (bqty - take, bprice)

        avg_buy = matched_buy_isk / matched_qty if matched_qty else 0.0

        # Allocate fees proportionally by sell ISK
        item_sell_isk = total_sell_isk_type
        fees = (item_sell_isk / total_sell_isk * total_fees) if total_sell_isk else 0.0

        net_profit = gross_profit - fees
        margin_pct = (net_profit / item_sell_isk * 100) if item_sell_isk else 0.0

        results.append({
            "type_id":     type_id,
            "type_name":   type_name,
            "qty_bought":  qty_bought,
            "qty_sold":    qty_sold,
            "avg_buy":     round(avg_buy, 2),
            "avg_sell":    round(avg_sell, 2),
            "gross_profit": round(gross_profit, 2),
            "fees":        round(fees, 2),
            "net_profit":  round(net_profit, 2),
            "margin_pct":  round(margin_pct, 2),
        })

    results.sort(key=lambda r: r["net_profit"], reverse=True)
    return results


def compute_cancelled_order_losses(orders: list[dict]) -> list[dict]:
    """
    Returns cancelled/expired sell orders as potential lost capital.
    Each entry: {type_id, type_name, cancelled_qty, price, capital_at_risk, state}
    """
    result = []
    for o in orders:
        if o.get("state") not in ("cancelled", "expired"):
            continue
        if o.get("is_buy_order"):
            continue
        qty   = o.get("volume_remain", 0)
        price = o.get("price", 0.0)
        result.append({
            "type_id":        o["type_id"],
            "type_name":      o.get("type_name", str(o["type_id"])),
            "cancelled_qty":  qty,
            "price":          price,
            "capital_at_risk": round(qty * price, 2),
            "state":          o["state"],
            "issued":         o.get("issued", ""),
        })
    result.sort(key=lambda r: r["capital_at_risk"], reverse=True)
    return result


# Ship group IDs by activity type
_MINING_SHIP_GROUPS    = {463, 543, 1022, 883}   # Barge, Exhumer, Mining Frigate, Orca/Rorqual
_EXPLORATION_SHIP_GROUPS = {830, 1534, 831}        # Covert Ops, Expedition Frigate, EAF


def detect_session_type(ship_group_id: int | None) -> str:
    """Classify a session by the ship flown at session start."""
    if ship_group_id in _MINING_SHIP_GROUPS:
        return "Mining"
    if ship_group_id in _EXPLORATION_SHIP_GROUPS:
        return "Exploration"
    return "Combat"
