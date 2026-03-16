"""Hourly snapshot orchestration."""

from concurrent.futures import ThreadPoolExecutor
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

    token = auth.get_valid_token(character_id)
    esi = ESIClient(access_token=token)

    # --- Fetch raw ESI data ---
    public_info    = _safe(lambda: esi.get_public_info(character_id), {})
    wallet_balance = _safe(lambda: esi.get_wallet(character_id), 0.0)
    journal_raw    = _safe(lambda: esi.get_wallet_journal(character_id), [])
    location       = _safe(lambda: esi.get_location(character_id), {})
    ship           = _safe(lambda: esi.get_ship(character_id), {})
    skills_data    = _safe(lambda: esi.get_skills(character_id), {})
    online_info    = _safe(lambda: esi.get_online(character_id), {})
    all_assets     = _safe(lambda: esi.get_assets_all(character_id), [])
    market_prices  = _safe(lambda: esi.get_market_prices(), [])

    # Resolve system info
    system_id   = location.get("solar_system_id")
    system_info = _safe(lambda: esi.get_system_info(system_id), {}) if system_id else {}
    security_status = system_info.get("security_status")
    system_name     = system_info.get("name")

    # Resolve ship group
    ship_type_id  = ship.get("ship_type_id")
    type_info     = _safe(lambda: esi.get_type_info(ship_type_id), {}) if ship_type_id else {}
    ship_group_id = type_info.get("group_id")

    # Keep characters table current
    if public_info:
        store.upsert_character(character_id, public_info)

    # Online / session state
    is_online   = bool(online_info.get("online"))
    last_login  = online_info.get("last_login")
    last_logout = online_info.get("last_logout")

    # --- Previous snapshot for delta/session comparison ---
    prev = store.get_last_snapshot(character_id)

    # --- Session tracking via last_login / last_logout changes ---
    if prev:
        prev_last_login  = prev.get("last_login")
        prev_last_logout = prev.get("last_logout")

        # New login detected
        if last_login and last_login != prev_last_login:
            print(f"  [session] Login detected at {last_login}")
            store.save_session_start(
                character_id, last_login,
                ship_type_id, ship.get("ship_name"),
                system_id, system_name, security_status,
            )
            analytics.capture_session_start(character_id, {
                "started_at":      last_login,
                "ship_type_id":    ship_type_id,
                "ship_name":       ship.get("ship_name"),
                "solar_system_id": system_id,
                "system_name":     system_name,
                "security_status": security_status,
            })

        # New logout detected
        if last_logout and last_logout != prev_last_logout:
            print(f"  [session] Logout detected at {last_logout}")
            open_session = store.get_open_session(character_id)
            session_isk = 0.0
            duration_hours = None

            if open_session:
                session_journal = store.get_journal_entries_between(
                    character_id, open_session["started_at"], last_logout
                )
                session_isk = sum(e["amount"] for e in session_journal if e["amount"] > 0)
                try:
                    t0 = datetime.fromisoformat(open_session["started_at"].replace("Z", "+00:00"))
                    t1 = datetime.fromisoformat(last_logout.replace("Z", "+00:00"))
                    duration_hours = round((t1 - t0).total_seconds() / 3600, 2)
                except Exception:
                    pass
                store.close_session(character_id, open_session["started_at"], last_logout, session_isk)

            analytics.capture_session_end(character_id, {
                "ended_at":        last_logout,
                "isk_earned":      round(session_isk, 2),
                "duration_hours":  duration_hours,
                "ship_type_id":    ship_type_id,
                "ship_name":       ship.get("ship_name"),
                "solar_system_id": system_id,
                "system_name":     system_name,
                "security_status": security_status,
            })
            print(f"  [session] ISK earned this session: {session_isk:,.0f}")

    # --- Killmails ---
    recent_km_list = _safe(lambda: esi.get_killmails_recent(character_id), [])
    candidate_ids  = [km["killmail_id"] for km in recent_km_list]
    unseen_ids     = store.get_unseen_killmail_ids(character_id, candidate_ids)
    km_id_to_hash  = {km["killmail_id"]: km["killmail_hash"] for km in recent_km_list}

    new_killmails = []
    for km_id in unseen_ids:
        km_hash = km_id_to_hash[km_id]
        detail = _safe(lambda: esi.get_killmail(km_id, km_hash), {})
        if not detail:
            continue

        is_loss = int(detail.get("victim", {}).get("character_id") == character_id)
        lost_ship_type = detail.get("victim", {}).get("ship_type_id")
        km_system_id   = detail.get("solar_system_id")
        km_time        = detail.get("killmail_time", now)

        new_killmails.append({
            "killmail_id":     km_id,
            "killmail_hash":   km_hash,
            "kill_time":       km_time,
            "is_loss":         is_loss,
            "ship_type_id":    lost_ship_type,
            "solar_system_id": km_system_id,
        })

        if is_loss:
            # Resolve names for the loss event
            loss_system_info = _safe(lambda: esi.get_system_info(km_system_id), {}) if km_system_id else {}
            loss_type_info   = _safe(lambda: esi.get_type_info(lost_ship_type), {}) if lost_ship_type else {}
            print(f"  [loss] Ship lost: {loss_type_info.get('name', lost_ship_type)} in {loss_system_info.get('name', km_system_id)}")
            analytics.capture_ship_loss(character_id, {
                "kill_time":        km_time,
                "ship_type_id":     lost_ship_type,
                "ship_name":        loss_type_info.get("name"),
                "solar_system_id":  km_system_id,
                "system_name":      loss_system_info.get("name"),
                "security_status":  loss_system_info.get("security_status"),
                "killmail_id":      km_id,
            })

    if new_killmails:
        store.save_killmails(character_id, new_killmails)

    since_24h     = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    recent_losses = store.get_recent_losses(character_id, since_24h)

    # --- Save journal and skills ---
    if journal_raw:
        store.save_journal_entries(character_id, journal_raw)
    if skills_data.get("skills"):
        store.save_skills(character_id, skills_data["skills"])

    # Resolve and cache skill names (only for skills with NULL name)
    unnamed = [r[0] for r in store.conn.execute(
        "SELECT skill_id FROM skills WHERE character_id=? AND skill_name IS NULL",
        (character_id,)
    ).fetchall()]
    if unnamed:
        try:
            resolved = esi.get_universe_names(unnamed[:1000])
            name_map = {e["id"]: e["name"] for e in resolved if e.get("category") == "inventory_type"}
            store.update_skill_names(character_id, name_map)
            print(f"  [skills] Resolved {len(name_map)} skill names")
        except Exception as e:
            print(f"  [warn] Skill name resolution failed: {e}")

    # Resolve and cache skill group names (only for named skills missing a group)
    ungrouped = [r[0] for r in store.conn.execute(
        "SELECT skill_id FROM skills WHERE character_id=? AND skill_name IS NOT NULL AND group_name IS NULL",
        (character_id,)
    ).fetchall()]
    if ungrouped:
        try:
            def _fetch_skill_group(sid):
                try:
                    info = esi.get_type_info(sid)
                    gid  = info.get("group_id")
                    if gid:
                        return sid, esi.get_group_info(gid).get("name", "")
                except Exception:
                    pass
                return sid, None

            group_map = {}
            with ThreadPoolExecutor(max_workers=10) as pool:
                for sid, gname in pool.map(_fetch_skill_group, ungrouped[:500]):
                    if gname:
                        group_map[sid] = gname
            if group_map:
                store.update_skill_groups(character_id, group_map)
                print(f"  [skills] Resolved {len(group_map)} skill groups")
        except Exception as e:
            print(f"  [warn] Skill group resolution failed: {e}")

    # --- Compute metrics ---
    # ISK/hr = wallet delta between current and previous snapshot / elapsed hours.
    # This captures real net ISK flow (earnings minus expenditures) without
    # relying on journal parsing. Activity type still comes from journal.
    isk_rates     = None
    activity_type = "Unknown"

    if prev and prev.get("wallet_balance") is not None and wallet_balance is not None:
        prev_dt    = datetime.fromisoformat(prev["captured_at"])
        now_dt     = datetime.fromisoformat(now)
        elapsed_h  = max((now_dt - prev_dt).total_seconds() / 3600, 0.01)
        delta      = wallet_balance - prev["wallet_balance"]
        isk_hr     = round(delta / elapsed_h, 2)
        isk_rates  = {
            "isk_hour":          isk_hr,
            "isk_hour_bounty":   0.0,
            "isk_hour_trade":    0.0,
            "isk_hour_industry": 0.0,
            "isk_hour_other":    isk_hr if isk_hr > 0 else 0.0,
        }

    # Activity type from journal (last 24h)
    journal_24h = store.get_journal_entries_between(character_id, since_24h, now)
    if journal_24h:
        activity_type = detect_activity_type(journal_24h, since_24h)

    # --- Estimated total account value = wallet + assets at adjusted market price ---
    price_map = {p["type_id"]: p.get("adjusted_price", 0.0) for p in market_prices}
    asset_value = sum(
        a.get("quantity", 1) * price_map.get(a["type_id"], 0.0)
        for a in all_assets
    )
    estimated_value = round((wallet_balance or 0.0) + asset_value, 2)
    print(f"  [value] wallet={wallet_balance:,.0f}  assets={asset_value:,.0f}  total={estimated_value:,.0f}")

    # --- Wealth ISK/hr (unrealized gains rate) ---
    # Delta of total estimated account value between snapshots / elapsed hours
    wealth_isk_hour = 0.0
    if prev and prev.get("estimated_value") is not None:
        prev_est = prev["estimated_value"]
        prev_dt  = datetime.fromisoformat(prev["captured_at"])
        now_dt   = datetime.fromisoformat(now)
        elapsed_h = max((now_dt - prev_dt).total_seconds() / 3600, 0.01)
        wealth_isk_hour = round((estimated_value - prev_est) / elapsed_h, 2)

    # --- At-risk value: ship hull + fitted/cargo assets when in space ---
    # Character is in space if no station and no structure
    in_space = not location.get("station_id") and not location.get("structure_id")
    at_risk_value = 0.0
    if in_space and all_assets and market_prices:
        ship_item_id = ship.get("ship_item_id")
        # Ship hull value
        if ship_type_id:
            at_risk_value += price_map.get(ship_type_id, 0.0)
        # All assets located ON the ship (location_id = ship_item_id)
        if ship_item_id:
            for a in all_assets:
                if a.get("location_id") == ship_item_id:
                    at_risk_value += a.get("quantity", 1) * price_map.get(a["type_id"], 0.0)
        at_risk_value = round(at_risk_value, 2)
        print(f"  [risk] In space — at_risk_value={at_risk_value:,.0f}")

    risk_level = compute_risk_level(security_status, ship_group_id, recent_losses, at_risk_value)

    # --- Build and save snapshot ---
    snap = {
        "character_id":    character_id,
        "character_name":  public_info.get("name"),
        "corporation_id":  public_info.get("corporation_id"),
        "captured_at":     now,
        "wallet_balance":  wallet_balance,
        "solar_system_id": system_id,
        "system_name":     system_name,
        "station_id":      location.get("station_id"),
        "structure_id":    location.get("structure_id"),
        "ship_type_id":    ship_type_id,
        "ship_name":       ship.get("ship_name"),
        "ship_group_id":   ship_group_id,
        "security_status": security_status,
        "total_sp":        skills_data.get("total_sp"),
        "unallocated_sp":  skills_data.get("unallocated_sp"),
        "isk_hour":        isk_rates["isk_hour"] if isk_rates else 0.0,
        "isk_hour_bounty": isk_rates["isk_hour_bounty"] if isk_rates else 0.0,
        "isk_hour_trade":  isk_rates["isk_hour_trade"] if isk_rates else 0.0,
        "isk_hour_industry": isk_rates["isk_hour_industry"] if isk_rates else 0.0,
        "isk_hour_other":  isk_rates["isk_hour_other"] if isk_rates else 0.0,
        "risk_level":      risk_level,
        "activity_type":   activity_type,
        "recent_losses":   recent_losses,
        "online":           int(is_online),
        "last_login":       last_login,
        "last_logout":      last_logout,
        "estimated_value":  estimated_value,
        "wealth_isk_hour":  wealth_isk_hour,
        "at_risk_value":    at_risk_value,
    }

    store.save_snapshot(snap)
    analytics.capture_hourly_snapshot(character_id, snap)
    analytics.flush()

    print(f"[sync] Done — {public_info.get('name')} | {wallet_balance:,.0f} ISK | {'ONLINE' if is_online else 'offline'} | risk: {risk_level} | activity: {activity_type}")
    return snap
