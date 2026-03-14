"""
Backfill historical ISK data from wallet journal.

Groups journal entries by ISO week, reconstructs approximate wallet balance
going backwards from the current known balance, and inserts weekly snapshots
into the DB so the dashboard shows historical trends.
"""

import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

load_dotenv()

from src.eve_client.auth import EveAuth
from src.eve_client.esi import ESIClient
from src.eve_client.metrics import (
    BOUNTY_TYPES,
    INDUSTRY_TYPES,
    MISSION_TYPES,
    TRADE_TYPES,
)
from src.eve_client.store import SnapshotStore

CHARACTER_ID = int(os.environ["EVE_CHARACTER_ID"])
WEEKS_BACK = 26  # 6 months


def week_key(date_str: str) -> str:
    """Return ISO year-week string, e.g. '2025-W42'."""
    dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    iso = dt.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def week_end_dt(year_week: str) -> datetime:
    """Return the Sunday 23:59:59 UTC of a given ISO week string."""
    year, week = year_week.split("-W")
    # ISO week Monday = day 1, Sunday = day 7
    monday = datetime.fromisocalendar(int(year), int(week), 1).replace(tzinfo=timezone.utc)
    sunday = monday + timedelta(days=6, hours=23, minutes=59, seconds=59)
    return sunday


def categorize(ref_type: str) -> str:
    if ref_type in BOUNTY_TYPES or ref_type in MISSION_TYPES:
        return "bounty"
    if ref_type in TRADE_TYPES:
        return "trade"
    if ref_type in INDUSTRY_TYPES:
        return "industry"
    return "other"


def main():
    auth = EveAuth(
        client_id=os.environ["EVE_CLIENT_ID"],
        client_secret=os.environ["EVE_CLIENT_SECRET"],
        callback_url=os.environ["EVE_CALLBACK_URL"],
    )
    if not auth._token:
        print("No token found. Run main.py first to authenticate.")
        return

    store = SnapshotStore("eve_snapshots.db")
    token = auth.get_valid_token()
    esi = ESIClient(access_token=token)

    # Get current wallet balance as our anchor point
    current_balance = esi.get_wallet(CHARACTER_ID)
    public_info = esi.get_public_info(CHARACTER_ID)
    print(f"Current wallet: {current_balance:,.0f} ISK")
    print("Fetching full wallet journal (all pages)…")

    entries = esi.get_wallet_journal_all(CHARACTER_ID)
    print(f"Total entries fetched: {len(entries):,}")

    # Cutoff: 6 months ago
    cutoff = datetime.now(timezone.utc) - timedelta(weeks=WEEKS_BACK)
    entries = [e for e in entries if datetime.fromisoformat(
        e["date"].replace("Z", "+00:00")) >= cutoff]
    print(f"Entries within last {WEEKS_BACK} weeks: {len(entries):,}")

    # Group by ISO week
    weeks: dict[str, list[dict]] = defaultdict(list)
    for e in entries:
        weeks[week_key(e["date"])].append(e)

    # Sort weeks chronologically
    sorted_weeks = sorted(weeks.keys())
    print(f"Weeks with data: {len(sorted_weeks)}")

    # Reconstruct balance going backwards from now.
    # net_change[week] = sum of all journal amounts that week.
    net_by_week = {}
    for wk, wk_entries in weeks.items():
        net_by_week[wk] = sum(e["amount"] for e in wk_entries)

    # Walk backwards: balance at end of each week
    # current_balance is the balance right now (after the most recent week)
    balance = current_balance
    week_end_balances = {}
    for wk in reversed(sorted_weeks):
        week_end_balances[wk] = balance
        balance -= net_by_week[wk]  # subtract this week's net to get previous week's balance

    # Insert weekly snapshots
    print("\nInserting weekly snapshots…")
    print(f"{'Week':<12} {'Balance':>18} {'Net Change':>16} {'Bounty/hr':>12} {'Trade/hr':>12}")
    print("-" * 75)

    inserted = 0
    for wk in sorted_weeks:
        wk_entries = weeks[wk]
        end_dt = week_end_dt(wk)
        captured_at = end_dt.isoformat()

        # Skip if we already have a snapshot within this week
        existing = store.conn.execute(
            "SELECT id FROM snapshots WHERE character_id=? AND captured_at LIKE ?",
            (CHARACTER_ID, f"{end_dt.strftime('%Y-%m-%d')}%"),
        ).fetchone()
        if existing:
            continue

        # Per-category ISK totals (income only)
        cat_totals = defaultdict(float)
        for e in wk_entries:
            if e["amount"] > 0:
                cat_totals[categorize(e["ref_type"])] += e["amount"]

        hours = 168.0  # 7 days
        net = net_by_week[wk]
        bal = week_end_balances[wk]

        snap = {
            "character_id": CHARACTER_ID,
            "character_name": public_info.get("name"),
            "corporation_id": public_info.get("corporation_id"),
            "captured_at": captured_at,
            "wallet_balance": bal,
            "solar_system_id": None,
            "system_name": None,
            "station_id": None,
            "structure_id": None,
            "ship_type_id": None,
            "ship_name": None,
            "ship_group_id": None,
            "security_status": None,
            "total_sp": None,
            "unallocated_sp": None,
            "isk_hour": round(sum(cat_totals.values()) / hours, 2),
            "isk_hour_bounty": round(cat_totals["bounty"] / hours, 2),
            "isk_hour_trade": round(cat_totals["trade"] / hours, 2),
            "isk_hour_industry": round(cat_totals["industry"] / hours, 2),
            "isk_hour_other": round(cat_totals["other"] / hours, 2),
            "risk_level": None,
            "activity_type": max(cat_totals, key=cat_totals.get) if cat_totals else "Unknown",
            "recent_losses": 0,
        }

        store.save_snapshot(snap)
        store.save_journal_entries(CHARACTER_ID, wk_entries)
        inserted += 1

        print(f"{wk:<12} {bal:>18,.0f} {net:>+16,.0f} {snap['isk_hour_bounty']:>12,.0f} {snap['isk_hour_trade']:>12,.0f}")

    print(f"\nDone. Inserted {inserted} weekly snapshots.")


if __name__ == "__main__":
    main()
