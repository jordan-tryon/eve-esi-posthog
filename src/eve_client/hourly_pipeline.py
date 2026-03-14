"""Hourly snapshot orchestration."""

from datetime import datetime, timedelta, timezone

from .analytics import Analytics
from .auth import EveAuth
from .esi import ESIClient
from .metrics import compute_isk_rates, compute_risk_level, detect_activity_type
from .store import SnapshotStore


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe(fn, default=None):
    try:
        return fn()
    except Exception as e:
        print(f"  [warn] {e}")
        return default


def run_hourly_snapshot(
    character_id: int,
    auth: EveAuth,
    analytics: Analytics,
    store: SnapshotStore,
):
    now = _utcnow()
    print(f"[sync] {now} — character {character_id}")

    token = auth.get_valid_token()
    esi = ESIClient(access_token=token)

    # --- Fetch raw ESI data ---
    public_info = _safe(lambda: esi.get_public_info(character_id), {})
    wallet_balance = _safe(lambda: esi.get_wallet(character_id))
    journal_raw = _safe(lambda: esi.get_wallet_journal(character_id), [])
    location = _safe(lambda: esi.get_location(character_id), {})
    ship = _safe(lambda: esi.get_ship(character_id), {})
    skills_data = _safe(lambda: esi.get_skills(character_id), {})

    # Resolve system info
    system_id = location.get("solar_system_id")
    system_info = _safe(lambda: esi.get_system_info(system_id), {}) if system_id else {}
    security_status = system_info.get("security_status")
    system_name = system_info.get("name")

    # Resolve ship group
    ship_type_id = ship.get("ship_type_id")
    type_info = _safe(lambda: esi.get_type_info(ship_type_id), {}) if ship_type_id else {}
    ship_group_id = type_info.get("group_id")

    # --- Killmails ---
    recent_km_list = _safe(lambda: esi.get_killmails_recent(character_id), [])
    candidate_ids = [km["killmail_id"] for km in recent_km_list]
    unseen_ids = store.get_unseen_killmail_ids(character_id, candidate_ids)
    km_id_to_hash = {km["killmail_id"]: km["killmail_hash"] for km in recent_km_list}

    new_killmails = []
    for km_id in unseen_ids:
        detail = _safe(lambda: esi.get_killmail(km_id, km_id_to_hash[km_id]), {})
        if detail:
            is_loss = int(detail.get("victim", {}).get("character_id") == character_id)
            new_killmails.append({
                "killmail_id": km_id,
                "killmail_hash": km_id_to_hash[km_id],
                "kill_time": detail.get("killmail_time", now),
                "is_loss": is_loss,
                "ship_type_id": detail.get("victim", {}).get("ship_type_id"),
                "solar_system_id": detail.get("solar_system_id"),
            })

    if new_killmails:
        store.save_killmails(character_id, new_killmails)

    since_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    recent_losses = store.get_recent_losses(character_id, since_24h)

    # --- Save journal entries ---
    if journal_raw:
        store.save_journal_entries(character_id, journal_raw)

    # --- Compute metrics from previous snapshot ---
    prev = store.get_last_snapshot(character_id)
    isk_rates = None
    activity_type = "Unknown"

    if prev:
        journal_window = store.get_journal_entries_between(character_id, prev["captured_at"], now)
        isk_rates = compute_isk_rates(journal_window, prev["captured_at"], now)
        activity_type = detect_activity_type(journal_window, prev["captured_at"])

    risk_level = compute_risk_level(security_status, ship_group_id, recent_losses)

    # --- Build and save snapshot ---
    snap = {
        "character_id": character_id,
        "character_name": public_info.get("name"),
        "corporation_id": public_info.get("corporation_id"),
        "captured_at": now,
        "wallet_balance": wallet_balance,
        "solar_system_id": system_id,
        "system_name": system_name,
        "station_id": location.get("station_id"),
        "structure_id": location.get("structure_id"),
        "ship_type_id": ship_type_id,
        "ship_name": ship.get("ship_name"),
        "ship_group_id": ship_group_id,
        "security_status": security_status,
        "total_sp": skills_data.get("total_sp"),
        "unallocated_sp": skills_data.get("unallocated_sp"),
        "isk_hour": isk_rates["isk_hour"] if isk_rates else None,
        "isk_hour_bounty": isk_rates["isk_hour_bounty"] if isk_rates else None,
        "isk_hour_trade": isk_rates["isk_hour_trade"] if isk_rates else None,
        "isk_hour_industry": isk_rates["isk_hour_industry"] if isk_rates else None,
        "isk_hour_other": isk_rates["isk_hour_other"] if isk_rates else None,
        "risk_level": risk_level,
        "activity_type": activity_type,
        "recent_losses": recent_losses,
    }

    store.save_snapshot(snap)

    # --- Push to PostHog ---
    analytics.capture_hourly_snapshot(character_id, snap)
    analytics.flush()

    print(f"[sync] Done — {public_info.get('name')} | wallet: {wallet_balance:,.0f} ISK | risk: {risk_level} | activity: {activity_type}")
    return snap
