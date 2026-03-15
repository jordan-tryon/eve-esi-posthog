"""EVE Character Tracker — public multi-character web app."""

import os
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

load_dotenv()

from src.eve_client.analytics import Analytics
from src.eve_client.auth import EveAuth
from src.eve_client.esi import ESIClient
from src.eve_client.hourly_pipeline import run_hourly_snapshot
from src.eve_client.store import SnapshotStore

# ── Globals ───────────────────────────────────────────────────────────────────

store = SnapshotStore("eve_snapshots.db")

auth = EveAuth(
    client_id=os.environ["EVE_CLIENT_ID"],
    client_secret=os.environ["EVE_CLIENT_SECRET"],
    callback_url=os.environ["EVE_CALLBACK_URL"],
    store=store,
)
analytics = Analytics(
    api_key=os.environ["POSTHOG_API_KEY"],
    host=os.environ.get("POSTHOG_HOST", "https://us.i.posthog.com"),
)

scheduler  = BackgroundScheduler()
_sync_jobs: dict[int, dict] = {}   # character_id → {running, last, error}


# ── Scheduler helpers ─────────────────────────────────────────────────────────

def _make_sync_fn(character_id: int):
    def _sync():
        _sync_jobs.setdefault(character_id, {})["running"] = True
        _sync_jobs[character_id]["error"] = None
        try:
            run_hourly_snapshot(character_id, auth, analytics, store)
            _sync_jobs[character_id]["last"] = datetime.now(timezone.utc).isoformat()
        except Exception as e:
            _sync_jobs[character_id]["error"] = str(e)
            print(f"[scheduler] Error syncing {character_id}: {e}")
        finally:
            _sync_jobs[character_id]["running"] = False
    return _sync


def register_character_job(character_id: int, run_now: bool = True):
    job_id = f"sync_{character_id}"
    if scheduler.get_job(job_id):
        return
    scheduler.add_job(
        _make_sync_fn(character_id),
        trigger=IntervalTrigger(hours=1),
        id=job_id,
        next_run_time=datetime.now(timezone.utc) if run_now else None,
    )
    _sync_jobs[character_id] = {"running": False, "last": None, "error": None}
    print(f"[scheduler] Registered job for character {character_id}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Schedule all already-registered characters
    for cid in store.get_all_character_ids():
        register_character_job(cid, run_now=True)

    if not scheduler.running:
        scheduler.start()
    print(f"[app] Scheduler started with {len(store.get_all_character_ids())} character(s).")
    yield
    scheduler.shutdown(wait=False)


# ── FastAPI ───────────────────────────────────────────────────────────────────

app       = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")


# ── Auth ──────────────────────────────────────────────────────────────────────

@app.get("/auth/start")
def auth_start():
    return RedirectResponse(auth.get_auth_url())


@app.get("/auth/callback")
@app.get("/callback")
def auth_callback(code: str):
    character_id = auth.exchange_code(code)
    if not scheduler.running:
        scheduler.start()
    register_character_job(character_id, run_now=True)
    return RedirectResponse(f"/c/{character_id}")


# ── Public pages ──────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
def landing(request: Request):
    characters  = store.get_all_characters()
    recent_snaps = []
    recent_losses = []

    for ch in characters:
        snap = store.get_last_snapshot(ch["character_id"])
        if snap:
            recent_snaps.append(snap)

    # Recent losses across all characters
    rows = store.conn.execute(
        """SELECT k.*, c.character_name
           FROM killmails k
           LEFT JOIN characters c ON k.character_id = c.character_id
           WHERE k.is_loss = 1
           ORDER BY k.kill_time DESC LIMIT 20"""
    ).fetchall()
    recent_losses = [dict(r) for r in rows]

    return templates.TemplateResponse("landing.html", {
        "request": request,
        "characters": characters,
        "recent_snaps": {s["character_id"]: s for s in recent_snaps},
        "recent_losses": recent_losses,
    })


@app.get("/c/{character_id}", response_class=HTMLResponse)
def character_page(request: Request, character_id: int):
    character = store.get_character(character_id)
    if not character:
        return HTMLResponse("<h2>Character not tracked. <a href='/auth/start'>Add yours?</a></h2>", status_code=404)
    current   = store.get_last_snapshot(character_id) or {}
    snapshots = store.get_snapshots(character_id, limit=168)
    sessions  = store.conn.execute(
        "SELECT * FROM sessions WHERE character_id=? ORDER BY started_at DESC LIMIT 20",
        (character_id,)
    ).fetchall()
    losses = store.conn.execute(
        "SELECT * FROM killmails WHERE character_id=? AND is_loss=1 ORDER BY kill_time DESC LIMIT 20",
        (character_id,)
    ).fetchall()
    status = _sync_jobs.get(character_id, {})
    skills = store.get_skills(character_id)
    return templates.TemplateResponse("character.html", {
        "request":     request,
        "character":   character,
        "current":     current,
        "snapshots":   snapshots,
        "sessions":    [dict(s) for s in sessions],
        "losses":      [dict(l) for l in losses],
        "sync_status": status,
        "skills":      skills,
    })


@app.get("/skills/{character_id}", response_class=HTMLResponse)
def skills_page(request: Request, character_id: int):
    character = store.get_character(character_id)
    if not character:
        return RedirectResponse("/")
    current    = store.get_last_snapshot(character_id) or {}
    skill_rows = store.get_skills(character_id)
    return templates.TemplateResponse("skills.html", {
        "request":      request,
        "current":      current,
        "skills":       skill_rows,
        "character_id": character_id,
        "character":    character,
    })


# ── API ───────────────────────────────────────────────────────────────────────

@app.get("/api/characters")
def api_characters():
    chars  = store.get_all_characters()
    result = []
    for ch in chars:
        snap = store.get_last_snapshot(ch["character_id"])
        result.append({**ch, "latest_snapshot": snap})
    return result


@app.get("/api/c/{character_id}/current")
def api_current(character_id: int):
    return store.get_last_snapshot(character_id) or {}


@app.get("/api/c/{character_id}/history")
def api_history(character_id: int, hours: int = 168):
    snaps = store.get_snapshots(character_id, limit=hours)
    snaps.reverse()
    return snaps


@app.get("/api/c/{character_id}/fitting")
def api_fitting(character_id: int):
    token = auth.get_valid_token(character_id)
    esi   = ESIClient(access_token=token)

    ship       = esi.get_ship(character_id)
    ship_item  = ship["ship_item_id"]
    type_info  = esi.get_type_info(ship["ship_type_id"])
    all_assets = esi.get_assets_all(character_id)
    fitted     = [a for a in all_assets if a.get("location_id") == ship_item]
    prices_raw = esi.get_market_prices()
    price_map  = {p["type_id"]: p.get("adjusted_price", 0.0) for p in prices_raw}

    type_ids = list({a["type_id"] for a in fitted})
    names    = {
        e["id"]: e["name"]
        for e in esi.get_universe_names(type_ids)
        if e.get("category") == "inventory_type"
    }

    SLOT_GROUPS = {
        "high":  [f"HiSlot{i}"  for i in range(8)],
        "mid":   [f"MedSlot{i}" for i in range(8)],
        "low":   [f"LoSlot{i}"  for i in range(8)],
        "rig":   [f"RigSlot{i}" for i in range(3)],
        "drone": ["DroneBay"],
        "cargo": ["Cargo"],
    }
    slots = defaultdict(list)
    for item in fitted:
        flag     = item.get("location_flag", "Other")
        name     = names.get(item["type_id"], str(item["type_id"]))
        qty      = item.get("quantity", 1)
        val      = round(qty * price_map.get(item["type_id"], 0.0), 2)
        for group, flags in SLOT_GROUPS.items():
            if flag in flags:
                slots[group].append({"name": name, "qty": qty, "flag": flag, "value": val})
                break
        else:
            slots["other"].append({"name": name, "qty": qty, "flag": flag, "value": val})

    hull_value  = round(price_map.get(ship["ship_type_id"], 0.0), 2)
    slot_totals = {grp: round(sum(i["value"] for i in items), 2) for grp, items in slots.items()}
    total_value = round(hull_value + sum(slot_totals.values()), 2)

    location = esi.get_location(character_id)
    in_space = not location.get("station_id") and not location.get("structure_id")

    return {
        "ship_name":       ship["ship_name"],
        "ship_type":       type_info.get("name"),
        "ship_type_id":    ship["ship_type_id"],
        "hull_value":      hull_value,
        "total_value":     total_value,
        "slot_totals":     slot_totals,
        "slots":           dict(slots),
        "in_space":        in_space,
    }


def _extract_skill_reqs(type_info: dict) -> list[dict]:
    """Extract required skills from dogma attributes."""
    SKILL_MAP = {182:277, 183:278, 184:279, 1285:1286, 1289:1287, 1290:1288}
    attrs = {a['attribute_id']: a['value'] for a in type_info.get('dogma_attributes', [])}
    reqs = []
    for skill_attr, level_attr in SKILL_MAP.items():
        if skill_attr in attrs and level_attr in attrs:
            sid = int(attrs[skill_attr])
            lvl = int(attrs[level_attr])
            if sid > 0 and lvl > 0:
                reqs.append({"skill_id": sid, "level": lvl})
    return reqs


@app.get("/api/c/{character_id}/optimal")
def api_optimal(character_id: int):
    token = auth.get_valid_token(character_id)
    esi   = ESIClient(access_token=token)

    # Get current ship + fitted modules
    ship       = esi.get_ship(character_id)
    ship_item  = ship["ship_item_id"]
    all_assets = esi.get_assets_all(character_id)
    fitted     = [a for a in all_assets if a.get("location_id") == ship_item]

    # Collect all type_ids: ship hull + fitted modules (exclude cargo)
    CARGO_FLAGS = {"Cargo", "DroneBay"}
    module_type_ids = {ship["ship_type_id"]}
    for a in fitted:
        if a.get("location_flag") not in CARGO_FLAGS:
            module_type_ids.add(a["type_id"])

    # Fetch type info for each and extract skill requirements
    # Aggregate: for each skill, take the MAX required level across all items
    all_reqs: dict[int, dict] = {}  # skill_id -> {level, items: [name]}
    item_names = {}
    for tid in module_type_ids:
        try:
            info = esi.get_type_info(tid)
            item_names[tid] = info.get("name", str(tid))
            for req in _extract_skill_reqs(info):
                sid = req["skill_id"]
                if sid not in all_reqs or req["level"] > all_reqs[sid]["level"]:
                    all_reqs[sid] = {"level": req["level"], "required_by": item_names[tid]}
        except Exception:
            pass

    # Resolve skill requirement names
    req_skill_ids = list(all_reqs.keys())
    skill_names: dict[int, str] = {}
    if req_skill_ids:
        try:
            resolved = esi.get_universe_names(req_skill_ids)
            skill_names = {e["id"]: e["name"] for e in resolved if e.get("category") == "inventory_type"}
        except Exception:
            pass

    # Get character's trained skills
    char_skills = store.get_skills_map(character_id)

    # Build results
    results = []
    total_score = 0.0
    max_score   = 0.0

    for sid, req in all_reqs.items():
        required = req["level"]
        trained  = char_skills.get(sid, 0)
        score    = min(trained, required) / required
        total_score += score
        max_score   += 1.0
        results.append({
            "skill_id":    sid,
            "skill_name":  skill_names.get(sid, f"Skill {sid}"),
            "required":    required,
            "trained":     trained,
            "met":         trained >= required,
            "required_by": req["required_by"],
        })

    results.sort(key=lambda r: (r["met"], r["trained"] - r["required"]))
    optimal_pct = round((total_score / max_score * 100) if max_score else 100, 1)

    return {
        "ship_name":   ship["ship_name"],
        "ship_type":   item_names.get(ship["ship_type_id"], "Unknown"),
        "optimal_pct": optimal_pct,
        "fully_optimal": optimal_pct >= 100.0,
        "requirements": results,
    }


@app.post("/api/c/{character_id}/sync")
def api_sync(character_id: int, background_tasks: BackgroundTasks):
    if not store.get_token(character_id):
        return JSONResponse({"error": "character not registered"}, status_code=404)
    if _sync_jobs.get(character_id, {}).get("running"):
        return JSONResponse({"status": "already_running"})
    # Mark running BEFORE handing off to background so status polls don't race
    _sync_jobs.setdefault(character_id, {})["running"] = True
    _sync_jobs[character_id]["error"] = None
    background_tasks.add_task(_make_sync_fn(character_id))
    return JSONResponse({"status": "started"})


@app.get("/api/c/{character_id}/status")
def api_status(character_id: int):
    return _sync_jobs.get(character_id, {"running": False, "last": None, "error": None})


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8080, reload=False)
