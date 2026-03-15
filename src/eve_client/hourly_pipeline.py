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

    token = auth.get_valid_token(character_id)
    esi = ESIClient(access_token=token)

    # --- Fetch raw ESI data ---
    public_info  = _safe(lambda: esi.get_public_info(character_id), {})
    wallet_balance = _safe(lambda: esi.get_wallet(character_id), 0.0)
    journal_raw  = _safe(lambda: esi.get_wallet_journal(character_id), [])
    location     = _safe(lambda: esi.get_location(character_id), {})
    ship         = _safe(lambda: esi.get_ship(character_id), {})
    skills_data  = _safe(lambda: esi.get_skills(character_id), {})
    online_info  = _safe(lambda: esi.get_online(character_id), {})

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

    # --- Save journal entries ---
    if journal_raw:
        store.save_journal_entries(character_id, journal_raw)

    # --- Compute metrics ---
    # Use a 24h rolling window so ISK/hr stays meaningful between syncs.
    # Fall back to the most recent session window if no 24h journal data.
    isk_rates     = None
    activity_type = "Unknown"

    journal_24h = store.get_journal_entries_between(character_id, since_24h, now)

    if journal_24h:
        isk_rates     = compute_isk_rates(journal_24h, since_24h, now)
        activity_type = detect_activity_type(journal_24h, since_24h)
    else:
        # No recent activity — find the most recent session and rate from that
        sessions = store.get_sessions(character_id, limit=1)
        if sessions:
            sess = sessions[0]
            sess_since = sess["started_at"]
            sess_until = sess["ended_at"] or now
            sess_journal = store.get_journal_entries_between(character_id, sess_since, sess_until)
            if sess_journal:
                isk_rates     = compute_isk_rates(sess_journal, sess_since, sess_until)
                activity_type = detect_activity_type(sess_journal, sess_since)

    risk_level = compute_risk_level(security_status, ship_group_id, recent_losses)

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
        "online":          int(is_online),
        "last_login":      last_login,
        "last_logout":     last_logout,
    }

    store.save_snapshot(snap)
    analytics.capture_hourly_snapshot(character_id, snap)
    analytics.flush()

    print(f"[sync] Done — {public_info.get('name')} | {wallet_balance:,.0f} ISK | {'ONLINE' if is_online else 'offline'} | risk: {risk_level} | activity: {activity_type}")
    return snap
